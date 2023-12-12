import os, re, json, csv, shutil, time, sys, requests, copy, subprocess, random, string,threading, geojson 
from datetime import date, timedelta, datetime
from shapely.geometry import shape
import pandas as pd
from multiprocess import Pool
from IPython.display import display
from google.cloud import storage
from threading import Thread
from json import JSONDecoder
from functools import partial
    
    
#-------------------------------------------------------------------------------------------------------------------------------
# GLOBAL VARIABLES
#-------------------------------------------------------------------------------------------------------------------------------

storage_client = storage.Client()
bucketName='mai_2023'
bucket=storage_client.get_bucket(bucketName)
order_url = "https://api.planet.com/compute/ops/orders/v2"
search_url = "https://api.planet.com/data/v1/quick-search"
with open('./MAI2023/cred.txt','r') as f:
    cred = f.readlines()
cred=cred[0]
file_lock = threading.Lock()
masterLocationFile_lock = threading.Lock()
session = requests.Session()
maxCloudCover = 0.5
colspecs = [(0, 24), (26, 38), (40, 82), (84, 93), (95, 1000)]
max_retries = 10
retry_interval =5
date_pattern1 = r'_20\d{2}-\d{2}-\d{2}_'
date_pattern2 = r'PSScene/20\d{2}\d{2}\d{2}_'
nodePath = str(subprocess.check_output(['which node'], shell=True))[2:-3]
eerunnerPath = str(subprocess.check_output(['which ee-runner'], shell=True))[2:-3]
    
    
#-------------------------------------------------------------------------------------------------------------------------------
# MASTER RUNNER
#-------------------------------------------------------------------------------------------------------------------------------

def run_master(locGroup, endDate, planet_api_key, private_key):
    
    session.auth = (planet_api_key, "")
    
    print("current in-scope variables:", dir())
    
    # Get a list of locs from the master location file
    locGroup_status = locationFileSummary(locGroup)
    locs = locGroup_status['Location'].tolist()
    print(f'Initiating download and processing for {locGroup} locs:', locs)

    ### Extract json of each feature, convert to convex hull geometry, and export to temp folder as a json
    
    #pull jsons from GCS
    data_string = bucket.get_blob(f'{locGroup}/{locGroup}.geojson').download_as_string()
    data = json.loads(data_string)

    for j in data['features']:
        loc = j['properties']['mktID'].replace(".", "_", 2) #fix name format of mktID
        dumped = json.dumps(j) #dump feature json into string format
        added = '{"type": "FeatureCollection", "features": [{' + dumped[1:-1] + '}]}' #add type classification to the string

        # Create a GeoJSON string and load it as a Shapely geometry
        geometry = geojson.loads(added)
        geometry_type = geometry['features'][0]['geometry']['type']
        shapely_geometry = shape(geometry['features'][0]['geometry'])

        if geometry_type == 'MultiPolygon':
            # Compute the convex hull for a MultiPolygon
            convex_hull = shapely_geometry.convex_hull
            convex_hull_geometry_type = 'Polygon'
        else:
            # Compute the convex hull for a Polygon
            convex_hull = shapely_geometry
            convex_hull_geometry_type = 'Polygon'

        # Convert the convex hull to GeoJSON
        convex_hull_geojson = geojson.Feature(geometry=convex_hull, properties={})

        # Save the convex hull as a GeoJSON file
        with open(f'./temp/Jsons/{loc}feature.geojson', 'w') as file:
            geojson.dump(convex_hull_geojson, file)
            
    ### initialize threads and start looping the downloader and processor functions
    
    #create threads for each function and pass variables as args
    downThread = threading.Thread(target = fn_downloader, args=(locGroup, locs, endDate, private_key))
    procThread = threading.Thread(target = fn_processor, args=(locGroup, locs))
    
    #start the threads
    downThread.start()
    procThread.start()

    #join the threads to wait for their completion
    downThread.join()
    procThread.join()
    
    print(f'Whoop whoop: {locGroup} done!')

    
#-------------------------------------------------------------------------------------------------------------------------------
# DOWNLOADER FUNCTIONS
#-------------------------------------------------------------------------------------------------------------------------------

def fn_downloader(locGroup, locs, endDate, private_key):
    #loop and request downloads 
    
    #make a list of locations that have not completed both types of download tasks
    locGroup_status = locationFileSummary(locGroup)
    notComplete = locGroup_status[(locGroup_status['00DownStatus'] != "complete") | (locGroup_status['00aDownNoSRStatus'] != "complete")]['Location'].tolist()
    
    while len(notComplete)!=0:
        
        #find locs whose imagery downloads are processing and check if they have finished: 
        for loc in locs:
            
            if (checkLocationFileStatus(locGroup, loc, "00DownStatus") == "initiated" or
                checkLocationFileStatus(locGroup, loc, "00aDownNoSRStatus") == "initiated"):
                
                fn_checkExistingImages(loc, locGroup, endDate)
        
        #pull the current locGroup summary and display it
        locGroup_status = locationFileSummary(locGroup)
        display(locGroup_status)
        
        #make a list of locations that have not completed both types of download tasks
        notComplete = locGroup_status[(locGroup_status['00DownStatus'] != "complete") | (locGroup_status['00aDownNoSRStatus'] != "complete")]['Location'].tolist()

        #pull a list of locs with currently-running downloads
        runningLocs = list(set(checkRunningOrders()))
        
        #filter the list such that we don't include currently running or queued tasks
        locsForDownload = [item for item in notComplete if item not in runningLocs]
        
        for loc in locsForDownload:
            
            requestDownloads(loc, locGroup, endDate, private_key)
            
            time.sleep(10)
            
    print(f'All downloads complete for {locGroup}!')

def requestDownloads(loc, locGroup, endDate, private_key):
    #Function to request the downloads needed for a given location.
    #Inputs: 
    #new_products [list]:      list of sr-corrected image product ids that should be downloaded from Planet
    #new_products_nosr [list]: list of non-sr-corrected image product ids that should be downloaded from Planet
    #forAnchoring [string]:    planet image product id that should be used for anchoring the downloaded images
    #private_key [string]:     file path to planet encrypted private key
    
    new_products, new_products_nosr, forAnchoring, GEEbucket = fn_checkExistingImages(loc, locGroup, endDate)
    print(f'New SR products found: {new_products[0:5]} ...')
    print(f'New no-SR products found: {new_products_nosr[0:5]} ...')
    orders = []
    loc_status = locationFileSummary(locGroup, loc)
    
    with open(f'./temp/Jsons/{loc}feature.geojson') as f:
        geojson_data = json.loads(f.read())
    lenCurrRunTasks =100
    #while lenCurrRunTasks>10:
    currRunnTasks = list(checkRunningOrders())
    lenCurrRunTasks = len(currRunnTasks)
    if lenCurrRunTasks>10:
        print(f'Currently running tasks: {lenCurrRunTasks} -- wait for some to finish before starting {loc}.')
        time.sleep(120)
    else:
        max_retries=10
        if len(new_products_nosr)>=5:
            for i in range(0, len(new_products_nosr), 499):
                print("Chunk", i, "no SR")
                itemIDs = []
                itemIDs.extend(new_products_nosr[i:i + 499])
                itemIDs.append(str(forAnchoring))

                order_payload_noSR = fn_order_payload_noSR()
                order_payload_noSR["products"][0]["item_ids"] = itemIDs
                order_payload_noSR["name"] = f'{loc} chunk {i} No-SR'
                order_payload_noSR["delivery"]["google_earth_engine"]["collection"] = f'PS_imgs/{locGroup}/{loc}'
                order_payload_noSR["delivery"]["google_earth_engine"]["project"] = GEEbucket
                order_payload_noSR["delivery"]["google_earth_engine"]["credentials"] = private_key
                order_payload_noSR["tools"][0]["clip"]['aoi'] = geojson_data["geometry"]
                order_payload_noSR["tools"][1]["coregister"]['anchor_item'] = forAnchoring

                #print('order_payload',order_payload)
                for attempt in range(max_retries + 1):
                    try:
                        order_response = session.post(order_url, json=order_payload_noSR)
                        if order_response.status_code == 202:
                            # Request succeeded, break out of the loop
                            break
                        else:
                            print(f"Request attempt {attempt + 1} failed with status code: {order_response.status_code}")
                    except Exception as e:
                        print(f"Request attempt {attempt + 1} failed with error: {e}")
                    if attempt < max_retries:
                        # Sleep before the next retry
                        time.sleep(retry_interval)
                    else:
                        print("Maximum retry attempts reached. Request failed.")
                orders.append(order_response.json())

                print('Order status code:', order_response.status_code)
                try:
                    print(loc+'Order ID:', order_response.json()['id'], '\n')
                except:
                    print('failed')

                if order_response.status_code != 202:
                    print(order_response.json())
                    if "Order request resulted in no acceptable assets" in json.dumps(order_response.json()) or "Unable to accept order: Cannot coregister single item. " in json.dumps(order_response.json()):
                        print(f'Non-SR download marked complete for {loc} since no new items were registered.')
                        updateLocationFileStatus(locGroup, loc, "00aDownNoSRStatus", "complete")

        if len(new_products)>=5:
            for i in range(0, len(new_products), 499):
                print("Chunk", i)
                itemIDs = []
                itemIDs.extend(new_products[i:i + 499])
                itemIDs.append(str(forAnchoring))

                order_payload =fn_order_payload()
                order_payload["products"][0]["item_ids"] = itemIDs
                order_payload["name"] = f'{loc} chunk {i}'
                order_payload["delivery"]["google_earth_engine"]["collection"] = f'PS_imgs/{locGroup}/{loc}'
                order_payload["delivery"]["google_earth_engine"]["project"] = GEEbucket
                order_payload["delivery"]["google_earth_engine"]["credentials"] = private_key
                order_payload["tools"][0]["clip"]['aoi'] = geojson_data["geometry"]
                order_payload["tools"][2]["coregister"]['anchor_item'] = forAnchoring

                #print('order_payload',order_payload)
                for attempt in range(max_retries + 1):
                    try:
                        order_response = session.post(order_url, json=order_payload)
                        if order_response.status_code == 202:
                            # Request succeeded, break out of the loop
                            break
                        else:
                            print(f"Request attempt {attempt + 1} failed with status code: {order_response.status_code}")
                    except Exception as e:
                        print(f"Request attempt {attempt + 1} failed with error: {e}")
                    if attempt < max_retries:
                        # Sleep before the next retry
                        time.sleep(retry_interval)
                    else:
                        print("Maximum retry attempts reached. Request failed.")
                orders.append(order_response.json())

                print('Order status code:', order_response.status_code)
                try:
                    print('Order ID:', order_response.json()['id'], '\n')
                except:
                    print('failed')

                if order_response.status_code != 202:
                    print(order_response.json())
                    if "Order request resulted in no acceptable assets" in json.dumps(order_response.json()) or "Unable to accept order: Cannot coregister single item. " in json.dumps(order_response.json()):
                        print(f'SR download marked complete for {loc} since no new items were registered.')
                        updateLocationFileStatus(locGroup, loc, "00DownStatus", "complete")
                #time.sleep(600) #1200

                print(f'SR download initiated for {loc}')
                updateLocationFileStatus(locGroup, loc, "00DownStatus", "initiated")
                time.sleep(60)

    #locationFileSummary(locGroup, loc)
    
def fn_checkExistingImages(loc, locGroup, endDate):
    # Takes a location, locGroup, and end date, and compares available Planet imagery to what is currently downloaded.
    # If this is the first time running for this loc, it creates a storage folder for the downloaded images.
    #
    # Inputs:
    # loc:      string (e.g: "lon14_115lat38_4743")
    # locGroup: string (e.g: "79_Tigray_1")
    # endDate:  string (e.g: "2023-09-30")
    #
    # Returns: 
    # new_products:      list of planet product IDs for SR images that we want, but don't have.
    # new_products_nosr: list of planet product IDs for non-SR images that we want, but don't have.
    # forAnchoring:      string of a planet product ID to use for anchoring (QUESTION: could re-making the anchor each time cause problems?
    # GEEbucket:         string of a generated GEE image bucket name for this location
    
    print("Checking existing images for", loc, "in", locGroup, "up to", endDate, ":")
    
    # Pull and display the row for the location
    loc_status = locationFileSummary(locGroup, loc)
    display(loc_status)
    
    # Extract GEE bucket name, and create a PS_imgs/locGroup folder for storing photos with a collection per location
    if locGroup=="Kenya":
        GEEbucket =loc_status["bucket"].item()
    else:
        GEEbucket =  'p' + loc_status["bucket"].item().replace('_','',5).lower()
    os.system(f'earthengine create folder projects/{GEEbucket}/assets/PS_imgs')
    os.system(f'earthengine create folder projects/{GEEbucket}/assets/PS_imgs/{locGroup}')
    os.system(f'earthengine create collection projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}')
    
    # If image downloads already marked complete, don't run.
    if loc_status["00aDownNoSRStatus"].item() =="complete" and loc_status["00DownStatus"].item() == "complete":
        print("All downloads marked complete for", loc, " -- skipping image check.")
        new_products, new_products_nosr, forAnchoring, GEEbucket = ('none', 'none', 'none', 'none')
    
    # If image downloads already running, don't run.
    locsRunningOrders = checkRunningOrders()
    print(f"locs with running Planet orders: {set(locsRunningOrders)}")
    if loc in locsRunningOrders:
        print("Downloads currently running for", loc, " -- skipping image check.")
        new_products, new_products_nosr, forAnchoring, GEEbucket = ('none', 'none', 'none', 'none')
              
    else:
        
        #search for collections that already exist for the location, and store them in a list
        pattern = r'{}/(.*?)_3B_AnalyticMS'.format(loc) #pattern to search for
        latestDate = "2016-01-01"
        name=f'{locGroup}/loc{loc}/'
        os.system(f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}"> ./temp/alreadyUp{loc}.txt')
        existing = []
        with open(f'./temp/alreadyUp{loc}.txt', "r") as file:
            for line in file:
                match = re.search(pattern, line.strip())
                if match:
                    existing.append(match.group(1))
                    
        with open(f'./temp/Jsons/{loc}feature.geojson') as f: 
            geojson_data = json.loads(f.read()) #open up the convex hull json
        
        ### Look for an anchor image
        # Search parameters for anchoring image
        search_para_1 = fn_search_para_1()
        search_para_1["filter"]["config"][0]["config"]["coordinates"] = geojson_data["geometry"]["coordinates"]
        search_para_1["filter"]["config"][1]["config"]["gte"] = "2020-01-01"+"T00:00:00Z"
        search_para_1["filter"]["config"][1]["config"]["lte"] = endDate+"T23:59:59Z"
        search_para_1["filter"]["config"][2]["config"]["lte"] = 0 # cloud cover
        search_para_1["filter"]["config"][3]["config"]["lte"] = 0 # anomalous_pixels
        search_para_1["filter"]["config"][4]["config"]["gte"] = 99 # clear_confidence_percent
        search_para_1["filter"]["config"][5]["config"]["gte"] = 99 # clear_percent
        search_para_1["filter"]["config"][6]["config"] = ["true"] # ground_control
        #print('search_para_1',search_para_1)
        
        # Search for anchor products using the Data API
        max_retries=10
        for attempt in range(max_retries + 1):
            try:
                search_response = session.post(search_url, json=search_para_1)
                if search_response.status_code == 200:
                    # Request succeeded, break out of the loop
                    break
                else:
                    print(f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}")
            except Exception as e:
                print(f"Request attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries:
                # Sleep before the next retry
                time.sleep(retry_interval)
            else:
                print("Maximum retry attempts reached. Request failed.")

        #print('Search status code:',search_response.status_code)
        #if search_response.status_code != 200:
        #    print(search_response.json())
        #    stop
        # Count number of features in first page
        geojson = search_response.json()
        features = geojson["features"]

        # Loop over all pages to count total number of features
        while True:
            next_link = geojson.get("_links", {}).get("_next")
            if next_link is None:
                break

            page_url = next_link
            for attempt in range(max_retries + 1):
                try:
                    r = session.get(page_url)
                    if r.status_code == 200:
                        # Request succeeded, break out of the loop
                        break
                    else:
                        print(f"Request attempt {attempt + 1} failed with status code: {r.status_code}")
                except Exception as e:
                    print(f"Request attempt {attempt + 1} failed with error: {e}")
                if attempt < max_retries:
                    # Sleep before the next retry
                    time.sleep(retry_interval)
                else:
                    print("Maximum retry attempts reached. Request failed.")
            geojson = r.json()
            features += geojson["features"]
        
        if len(features)==0:
            print("No image found for anchoring.")
            stop

        # Retrieve the product IDs from the search response
        product_ids = []
        for i in features:
            product_ids.append(i["id"]) #EDIT: 
        
        # Arbitrarily choose the last image for anchoring
        forAnchoring = product_ids[-1]

        # Create new search parameters to capture all images of interest     
        search_para_2 =fn_search_para_2()
        search_para_2["filter"]["config"][0]["config"]["coordinates"] = geojson_data["geometry"]["coordinates"]
        search_para_2["filter"]["config"][1]["config"]["gte"] = "2016-01-01T00:00:00Z"
        search_para_2["filter"]["config"][1]["config"]["lte"] = endDate+"T23:59:59Z"
        search_para_2["filter"]["config"][2]["config"]["lte"] = maxCloudCover
        #print('search_para_2',search_para_2)

        # Search for products using the Data API
        for attempt in range(max_retries + 1):
            try:
                search_response = session.post(search_url, json=search_para_2)
                if search_response.status_code == 200:
                    # Request succeeded, break out of the loop
                    break
                else:
                    print(f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}")
            except Exception as e:
                print(f"Request attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries:
                # Sleep before the next retry
                time.sleep(retry_interval)
            else:
                print("Maximum retry attempts reached. Request failed.")
        
        # Retrieve all features that are returned from the seatch
        geojson = search_response.json()
        features = geojson["features"]
        #print('geojson ', loc ,geojson)
        # Loop over all pages to count total number of features
        while True:
            next_link = geojson.get("_links", {}).get("_next")
            if next_link is None:
                break

            page_url = next_link
            for attempt in range(max_retries + 1):
                try:
                    r = session.get(page_url)
                    if r.status_code == 200:
                        # Request succeeded, break out of the loop
                        break
                    else:
                        print(f"Request attempt {attempt + 1} failed with status code: {r.status_code}")
                except Exception as e:
                    print(f"Request attempt {attempt + 1} failed with error: {e}")
                if attempt < max_retries:
                    # Sleep before the next retry
                    time.sleep(retry_interval)
                else:
                    print("Maximum retry attempts reached. Request failed.")
            geojson = r.json()
            features += geojson["features"]
        
        # Create lists of the SR and non-SR features that are returned from the search
        features_sr = [feature for feature in features if "ortho_analytic_4b_sr" in feature["assets"] and "ortho_udm2" in feature["assets"]]
              
        features_nosr = [feature for feature in features if "ortho_analytic_4b" in feature["assets"] and "ortho_analytic_4b_sr" not in feature["assets"] and "ortho_udm2" in feature["assets"]]

        # Retrieve the product IDs from the search response that we don't already have 
        product_ids = []
        for i in features_sr:
            product_ids.append(i["id"])
        new_products = remove_overlapping_strings(product_ids, existing)
        #print(f'total available SR products found for {loc}:', product_ids)
        #print(f'existing SR products found for {loc}:', existing)
        #print(f'new SR products deduced:', new_products)

        product_ids_nosr = []
        for i in features_nosr:
            product_ids_nosr.append(i["id"])
        new_products_nosr = remove_overlapping_strings(product_ids_nosr, existing)
        
        print(loc, 'total number of products available:', len(product_ids), 'sr,', len(product_ids_nosr), 'no-sr')
        print(loc, 'total number of existing products:', len(existing))
        print(loc, 'number of new products available:', len(new_products), 'sr,', len(new_products_nosr), 'no-sr')
        
        # If the number of new products is less than 5 for both download types, mark the location complete
        if len(new_products) <5 and len(new_products_nosr)< 5:
            print(f"All images already downloaded for {loc}")
            updateLocationFileStatus(locGroup, loc, "00DownStatus", "complete")
            updateLocationFileStatus(locGroup, loc, "00aDownNoSRStatus", "complete")
            locationFileSummary(locGroup, loc)

    return new_products, new_products_nosr, forAnchoring, GEEbucket


def checkRunningOrders(): # 
    with file_lock:
        #initialize list
        loc_list = []

        #create file of queued tasks
        os.system("planet orders list --state queued > planet_orders_output.txt")
        with open("planet_orders_output.txt", 'r') as file:    
            for line in file:
                #add loc name to list
                loc_list.append(json.loads(line)['name'].split(' ')[0])

        #create file of running tasks
        os.system("planet orders list --state running > planet_orders_output.txt")
        with open("planet_orders_output.txt", 'r') as file:    
            for line in file:
                #add loc name to list
                loc_list.append(json.loads(line)['name'].split(' ')[0]) 
        
    return loc_list


# ------------------------------------------------------------------------------------------------------------------------------  # PROCESSOR FUNCTIONS 
# ------------------------------------------------------------------------------------------------------------------------------
    
def fn_processor(locGroup,locList):
    #loops through a given list of locations within a locGroup to run checks and, if necessary, processing tasks
    #continues to loop continuously until all tasks have been concluded for all locations in the list
    
    #different setup combinations for analysis
    setups=[[50, 5]] #,[55,5],[60,5],[50,4],[55,4],[60,4],[50,6],[55,6],[60,6]
    if locGroup =="Kenya":
        country="Kenya"
    else:
        country = get_country_name(int(locGroup.split('_')[0]))
    
    #set up frequency based on input
    freqListStr="weekday" # weekday monthday, monthdayfromEnd, weekdayEverySecond,everyFiveDays
    if freqListStr=="weekday":
        freqList=6
        freqListStr_short = 'w7'
    elif freqListStr=="monthday" or freqListStr=="monthdayfromEnd":
        freqList=30
        freqListStr_short = 'w31'
    elif freqListStr=="weekdayEverySecond":
        freqList=13
        freqListStr_short = 'w14'
    elif freqListStr=="everyFiveDays":
        freqList=4
        freqListStr_short = 'w5'
        
    #create "processed" folder for each loc in the GEE bucket
    for loc in locList:
        loc_status = locationFileSummary(locGroup, loc)
        if locGroup=="Kenya":
            GEEbucket =loc_status["bucket"].item()
        else:
            GEEbucket =  'p' + loc_status["bucket"].item().replace('_','',5).lower()
        os.system(f'earthengine create folder projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}proc')
    
    #for each setup combination
    for setup in setups:
        hc = setup[1]
        fc = setup[0]
        
        #while there is processing left for any of the locations
        while checkCompletionStatus(locGroup, locList, fc, hc) == False:
            
            #loop through the locations
            for loc in locList:
                loc_status = locationFileSummary(locGroup, loc)

                #string for bucket name in GEE: ex: sierra_leone > psierraleone
                if locGroup=="Kenya":
                    GEEbucket =loc_status["bucket"].item()
                else:
                    GEEbucket =  'p' + loc_status["bucket"].item().replace('_','',5).lower()
                
                #if this loc has finished downloading imagery and hasn't yet completed processing
                if (checkCompletionStatus(locGroup, [loc], fc, hc) == False and
                    checkLocationFileStatus(locGroup, loc, "00DownStatus") == 'complete' and
                    checkLocationFileStatus(locGroup, loc, "00aDownNoSRStatus") == 'complete'):

                    print(f'beginning processing loop for {loc} in {locGroup}...')

                    #check if any tasks for this location and setup have failed due to empty geometry, and update
                    if len(checkFailedTasksGEE(loc, hc, fc, search = ['empty'])) != 0:

                        print(f'Tasks for {loc} in {locGroup} failed due to empty geometry -- updating location file')
                        updateLocationFileStatus(locGroup, loc, "02bMapFailed", f'{fc}_{hc}')
                        updateLocationFileStatus(locGroup, loc, "03bActFailed", f'{fc}_{hc}')
                    
                    #check if any activity for this location and setup have failed due to memory limits, and update
                    if len(checkFailedTasksGEE(loc, hc, fc, search = ['exceeded', 'timed out', 'Computed value'], prefix = 'exp_')) != 0:

                        print(f'Activity tasks for {loc} in {locGroup} failed due to computing limits -- updating location file')
                        updateLocationFileStatus(locGroup, loc, "03bActFailed", f'{fc}_{hc}')

                    #try each processing step
                    print(tryPropertiesExport(locGroup, loc, fc, hc, GEEbucket))
                    print(tryExportPrep(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country))
                    print(tryExportMarketShape(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country))
                    print(tryExportMarketActivity(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country))
                    
                    time.sleep(10)
            
            #wait in between loops through all the locations
            time.sleep(180)
            
        print(f'Processing tasks complete for {locList} in {locGroup} using HC {hc} and FC {fc}!')
    
    
def checkCompletionStatus(locGroup, locList, fc = '', hc = ''):
    #checks the processing task completion status of a list of locs within a locGroup. Returns False if any are 
    #incomplete, and True if all are complete

    for loc in locList:
        if ((f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "01aPrepComplete") or 
             f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "01bPrepFailed")) and
            (f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "02aMapComplete") or 
             f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "02bMapFailed")) and
            (f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "03aActComplete") or 
             f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "03bActFailed")) and
            checkLocationFileStatus(locGroup, loc, "04PropertiesDownload") in ['failed', 'complete']):
            pass
        else:
            return False
        
    return True


def checkFailedTasksGEE(loc = 'lon', hc = 5, fc = 50, search = ['empty'], prefix = '..._', freqListStr = ''):
    #checks GEE task history to look for tasks for the given location, fc, hc, and additional search string.
    #by default, will search for all tasks with hc = 5, fc = 50, and failure due to empty geometry
    
    failedTasks = re.findall(fr'({prefix}{loc}_FC{fc}_HC{hc}_{freqListStr}.*?)\\n',
                             str(subprocess.check_output(['earthengine task list --status FAILED'], shell=True)))
        
    return [line for line in failedTasks if any(term in line for term in search)]


def tryPropertiesExport(locGroup, loc, fc, hc, GEEbucket):
    #runs logical checks to see if properties export needs to be run for a given loc and locGroup, 
    #runs the export if necessary, and updates the location file accordingly.
        
    #initialize google storage client
    client = storage.Client()
    
    #if marked complete or failed, exit
    if (f'complete' in checkLocationFileStatus(locGroup, loc, "04PropertiesDownload") or 
        f'failed' in checkLocationFileStatus(locGroup, loc, "04PropertiesDownload")):
        
        return f'Properties export already concluded for {loc} in {locGroup} -- skipping.'

    #check if output already exists in the GCS bucket -- if so, exit and update
    if loc in str(list(client.list_blobs('exports-mai2023', prefix=f'{locGroup}/properties/'))):
        
        updateLocationFileStatus(locGroup, loc, '04PropertiesDownload', 'complete')
        return f'Properties export succeeded for {loc} in {locGroup} -- updating location file.'
    
    #check if task is already running -- if so, exit and update
    if (f'prop_{locGroup}_{loc}' in str(subprocess.check_output(['earthengine task list --status RUNNING'], shell=True)) or
        f'prop_{locGroup}_{loc}' in str(subprocess.check_output(['earthengine task list --status READY'], shell=True))):

        return f'Properties export currently running for {loc} in {locGroup} -- updating location file.'
    
    #set up and modify GEE code to prepare for running
    codeFile=f"./temp/_exportProp_{loc}.js" #delete after
    
    with open("./MAI/paul/imagePropertiesExporter", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace('INSERT_LOC_GROUP_HERE', locGroup)
                   .replace('"INSERT_LOC_HERE"', loc)
                   .replace('INSERT_BUCKET_HERE', GEEbucket)
                )
    
    #run the GEE code and update the location file
    subprocess.call([nodePath, eerunnerPath, codeFile])
    updateLocationFileStatus(locGroup, loc, "04PropertiesDownload", 'initiated')    
    return f"Properties download for {loc} in {locGroup} initiated  -- updating location file."
   
    
def tryExportPrep(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country):
    #runs logical checks to see if export prep needs to be run for a given loc and locGroup, 
    #runs the export prep if necessary, and updates the location file accordingly.
    
    #if marked complete or failed, skip
    if (f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "01aPrepComplete") or 
        f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "01bPrepFailed")):
        
        return f'01 Export prep already concluded for {loc} in {locGroup} -- skipping.'
    
    #check if output already exists in GEE -- if so, exit and update
    commandString = f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}proc"'
    
    if f'diffQbyWkd_r1_hc{hc}_fc{fc}' in str(subprocess.check_output([commandString], shell=True)):
         
        updateLocationFileStatus(locGroup, loc, '01aPrepComplete', f'{fc}_{hc}')
        updateLocationFileStatus(locGroup, loc, '01PrepStarted', f'{fc}_{hc}')
        return f'01 Export prep succeeded for {loc} in {locGroup} -- updating location file.'
    
    #check if task is already running -- if so, exit
    if (f'dif_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status RUNNING'], shell=True)) or
        f'dif_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status READY'], shell=True))):

        return f'01 Export prep currently running for {loc} in {locGroup}.'

    #set up and modify GEE code to prepare for running
    codeFile=f"./temp/_findMktsA_{loc}.js"
    with open("./MAI/latest/01_prepForExport_v20231016", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace('INSERT_LOC_GROUP_HERE', locGroup)
                   .replace('"INSERT_LOC_HERE"', loc)
                   .replace('"INSERT_HC_HERE"', str(hc))
                   .replace('"INSERT_FC_HERE"', str(fc))
                   .replace('"INSERT_FREQ_LIST_HERE"', str(freqList))
                   .replace('INSERT_FREQ_DAY_STR_HERE', freqListStr)
                   .replace('INSERT_BUCKET_HERE', GEEbucket)
                   .replace('INSERT_COUNTRY_HERE', country)
                )
                
    #run the GEE code and update the location file
    subprocess.call([nodePath, eerunnerPath, codeFile])
    updateLocationFileStatus(locGroup, loc, "01PrepStarted", str(fc)+"_"+str(hc))
    return f"01 Export prep for {loc} in {locGroup} started."
    
        
def tryExportMarketShape(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country):
    #runs logical checks to see if market shape export needs to be run for a given loc and locGroup, 
    #runs the export if necessary, and updates the location file accordingly.
       
    #initialize google storage client
    client = storage.Client()
    
    #if marked complete or failed, exit
    if (f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "02aMapComplete") or 
        f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "02bMapFailed")):
        
        return f'02 Map export already concluded for {loc} in {locGroup} -- skipping.'
    
    #if export prep task not yest complete, exit
    if f'{fc}_{hc}' not in checkLocationFileStatus(locGroup, loc, "01aPrepComplete"):
        
        return f'02 Map export for {loc} in {locGroup} cannot start since 01 export prep not complete -- skipping.'
    
    #check if output already exists in GCS and GEE -- if so, exit and update
    commandString = f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}proc"'

    if (f'shp{locGroup}{loc}' in str(subprocess.check_output([commandString], shell=True)) and 
        bool(re.search(fr'{loc}_As\d*_FC{fc}_HC{hc}', 
                       str(list(client.list_blobs('exports-mai2023', prefix=f'{locGroup}/shapes/')))))):

        updateLocationFileStatus(locGroup, loc, '02aMapComplete', f'{fc}_{hc}')
        updateLocationFileStatus(locGroup, loc, '02MapStarted', f'{fc}_{hc}')
        return f'02 Map export succeeded for {loc} in {locGroup} -- updating location file.'
    
    #check if task is already running -- if so, exit
    if (f'shp_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status RUNNING'], shell=True)) or
        f'shp_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status READY'], shell=True))):

        return f'02 Map export prep currently running for {loc} in {locGroup}.'

    #set up and modify GEE code to prepare for running
    codeFile=f"./temp/_findMktsB_{loc}.js"
    with open("./MAI/latest/02_exportMktShape_v20231016", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace('INSERT_LOC_GROUP_HERE', locGroup)
                   .replace('"INSERT_LOC_HERE"', loc)
                   .replace('"INSERT_HC_HERE"', str(hc))
                   .replace('"INSERT_FC_HERE"', str(fc))
                   .replace('"INSERT_FREQ_LIST_HERE"', str(freqList))
                   .replace('INSERT_FREQ_DAY_STR_HERE', freqListStr)
                   .replace('INSERT_BUCKET_HERE', GEEbucket)
                   .replace('INSERT_COUNTRY_HERE', country)
                ) 
    
    #run the GEE code and update the location file
    subprocess.call([nodePath, eerunnerPath, codeFile])
    updateLocationFileStatus(locGroup, loc, "02MapStarted", str(fc)+"_"+str(hc))
    return f"02 Map export for {loc} in {locGroup} started."
        
    

def tryExportMarketActivity(locGroup, loc, fc, hc, freqList, freqListStr, GEEbucket, country):
    #runs logical checks to see if market activity export needs to be run for a given loc and locGroup, 
    #runs the export if necessary, and updates the location file accordingly.
    
    #initialize google storage client
    client = storage.Client()
    
    #if marked complete or failed, exit
    if (f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "03aActComplete") or 
        f'{fc}_{hc}' in checkLocationFileStatus(locGroup, loc, "03bActFailed")):
        
        return f'03 Activity export already concluded for {loc} in {locGroup} -- skipping.'
    
    #if mapping asset from previous step not present, exit
    commandString = f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{locGroup}/{loc}proc"'
    
    if not bool(re.search(fr'shp{locGroup}{loc}_As\d*_FC{fc}_HC{hc}', str(subprocess.check_output([commandString], shell=True)))):     
        
        return f'03 Activity export asset not in GEE for {loc} in {locGroup} -- skipping.'
    
    #check if output already exists in GCS -- if so, exit and update
    if bool(re.search(fr'export{locGroup}{loc}_As\d*_FC{fc}_HC{hc}', str(list(client.list_blobs('exports-mai2023', prefix=f'{locGroup}/measures/'))))):

        updateLocationFileStatus(locGroup, loc, '03aActComplete', f'{fc}_{hc}')
        updateLocationFileStatus(locGroup, loc, '03ActStarted', f'{fc}_{hc}')
        return f'03 Activity export succeeded for {loc} in {locGroup} -- updating location file.'
    
    #check if task is already running -- if so, exit
    if (f'exp_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status RUNNING'], shell=True)) or
        f'exp_{loc}_FC{fc}_HC{hc}' in str(subprocess.check_output(['earthengine task list --status READY'], shell=True))):

        return f'03 Activity export currently running for {loc} in {locGroup}.'

    #set up and modify GEE code to prepare for running
    codeFile=f"./temp/_findMktsB_{loc}.js"
    with open("./MAI/latest/03_exportMktAct_v20231016", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace('INSERT_LOC_GROUP_HERE', locGroup)
                   .replace('"INSERT_LOC_HERE"', loc)
                   .replace('"INSERT_HC_HERE"', str(hc))
                   .replace('"INSERT_FC_HERE"', str(fc))
                   .replace('"INSERT_FREQ_LIST_HERE"', str(freqList))
                   .replace('INSERT_FREQ_DAY_STR_HERE', freqListStr)
                   .replace('INSERT_BUCKET_HERE', GEEbucket)
                   .replace('INSERT_COUNTRY_HERE', country)
                ) 

    #run the GEE code and update the location file
    subprocess.call([nodePath, eerunnerPath, codeFile])
    updateLocationFileStatus(locGroup, loc, "03ActStarted", str(fc)+"_"+str(hc)+";")
    return f"03 Activity export for {loc} started"

        
#-------------------------------------------------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#-------------------------------------------------------------------------------------------------------------------------------

def list_blobs_with_prefix(bucket_name, prefix, storage_client,delimiter=None): # This function lists all images in a specific GCS folder. We use it as an input into the following function, identifying images that we no longer need to download. 
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )
    return blobs

def remove_overlapping_strings(list1, list2): # This function checks for each of the strings in list1 whether it forms at least part of any of the strings in list2. We use it to filter out already downloaded imagery from the new set of tasks

    return [string for string in list1 if string not in '|'.join(list2)] #EDIT: used list comprehension to simplify and speed up
 
def extract_loc(input_string):
    match = re.search(r'(lon.*?)_FC', input_string)
    if match:
        return match.group(1)
    else:
        return ""
def extract_fc(input_string):
    match = re.search(r'_FC(.*?)_HC', input_string)
    if match:
        return match.group(1)
    else:
        return ""
def extract_hc(input_string):
    match = re.search(r'_HC(.*?)_w', input_string)
    if match:
        return match.group(1)
    else:
        return ""
    
def get_country_name(input_value):
    if input_value == 79:
        return "Ethiopia"
    elif input_value == 133:
        return "Kenya"
    else:
        return "Unknown"  # You can change this to a different value if desired
    
    
#-------------------------------------------------------------------------------------------------------------------------------
# API REQUEST PARAMETERS
#-------------------------------------------------------------------------------------------------------------------------------

def fn_search_para_1():
    search_para_1 = {
        "item_types": ["PSScene"],
        "filter": {
            "type": "AndFilter",
            "config": [
                {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": {
                        "type": "Polygon",
                        "coordinates": None  # Will be updated for each file
                    }
                },
                {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {
                        "gte": None,
                        "lte": None
                    }
                },
                {
                    "type": "RangeFilter",
                    "field_name": "cloud_cover",
                    "config": {
                        "lte": None
                    }
                },
                {
                    "type": "RangeFilter",
                    "field_name": "anomalous_pixels",
                    "config": {
                        "lte": None
                    }
                },
                {
                    "type": "RangeFilter",
                    "field_name": "clear_confidence_percent",
                    "config": {
                        "gte": None
                    }
                },
                {
                    "type": "RangeFilter",
                    "field_name": "clear_percent",
                    "config": {
                        "gte": None
                    }
                },
                {
                    "type": "StringInFilter",
                    "field_name": "ground_control",
                    "config": None
                },
                {
                    "type": "AssetFilter",
                    "config": [
                        "ortho_analytic_4b_sr"
                    ]
                },
                {
                    "type": "AssetFilter",
                    "config": [
                        "ortho_udm2"
                    ]
                },
                {
                    "type":"PermissionFilter",
                     "config":[
                        "assets.ortho_analytic_4b_sr:download"
                     ]
                }
            ]
        }
    }
    return search_para_1


# Data API search parameters
def fn_search_para_2():
    search_para_2 = {
        "item_types": ["PSScene"],
        "filter": {
            "type": "AndFilter",
            "config": [
                {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": {
                        "type": "Polygon",
                        "coordinates": None  # Will be updated for each file
                    }
                },
                {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {
                        "gte": None,
                        "lte": None
                    }
                },
                {
                    "type": "RangeFilter",
                    "field_name": "cloud_cover",
                    "config": {
                        "lte": None
                    }
                },
                {
                    "type": "AssetFilter",
                    "config": [
                        "ortho_analytic_4b_sr","ortho_analytic_4b"
                    ]
                },
                {
                    "type": "AssetFilter",
                    "config": [
                        "ortho_udm2"
                    ]
                }#,
                #{
                #    "type":"PermissionFilter",
                #     "config":[
                #        "assets.ortho_analytic_4b_sr:download"
                #     ]
                #}
            ]
        }
    }
    return search_para_2

# https://developers.planet.com/docs/apis/data/searches-filtering/#stringinfilter
# Order API parameters
def fn_order_payload():
    order_payload = {
        "name": None,#
        "order_type": "partial", # deliver only those items for which all parts of bundle are available
        "products": [{
            "item_ids": None, # to be filled in later
            "item_type": 'PSScene',
            "product_bundle": "analytic_sr_udm2"  # https://developers.planet.com/apis/orders/product-bundles-reference/
        }],
        "tools": [    # add or remove tools as needed
            {
                "clip": {
                    "aoi": None #
                }
            },
            {
                "harmonize": {
                    'target_sensor': 'Sentinel-2'
                }
            },
            {
                "coregister": {
                    "anchor_item": None # find the perfect image here (recent, little haze, little distortion)
                }
            }#,
            #{
            #  "file_format:" : {
            #      "format": "COG"
            #  }
            #}
        ],
        #"delivery": {
        #    "google_cloud_storage": {
        #        "bucket": None,
        #        "credentials": None,
        #        "path_prefix": None # locGroup+"/loc"+loc+"/"
        #    }
        "delivery": {
        "google_earth_engine": {
            "project": None,
            "collection": None,
            "credentials": None
        }
        }
    }
    return order_payload

def fn_order_payload_noSR():
    order_payload_noSR = {
        "name": None,#
        "order_type": "partial", # deliver only those items for which all parts of bundle are available
        "products": [{
            "item_ids": None, # to be filled in later
            "item_type": 'PSScene',
            "product_bundle": "analytic_udm2"  # https://developers.planet.com/apis/orders/product-bundles-reference/
        }],
        "tools": [    # add or remove tools as needed
            {
                "clip": {
                    "aoi": None #
                }
            },
            {
                "coregister": {
                    "anchor_item": None # find the perfect image here (recent, little haze, little distortion)
                }
            }
        ],
        "delivery": {
        "google_earth_engine": {
            "project": None,
            "collection": None,
            "credentials": None
        }
        }
    }
    return order_payload_noSR


# ------------------------------------------------------------------------------------------------------------------------------  # LOCATION FILE MANAGEMENT FUNCTIONS 
# ------------------------------------------------------------------------------------------------------------------------------
 
def checkLocationFileStatus(locGroup, loc, column, fileprefix = ''):
    #function to check the value of given and tracking column within the master location file
    #locGroup: string (e.g: "79_Tigray_1")
    #loc:      string (e.g: "lon14_115lat38_4743")
    #column:   string (e.g: "00DownStatus")
    #returns:  value in the specified column for the loc 
    
    with masterLocationFile_lock:
        df = pd.read_csv(f'{fileprefix}masterLocationFile{locGroup}.csv')
        return df.loc[df['Location'] == loc][column].item()

def updateLocationFileStatus(locGroup, loc, column, new_value, old_value = 'unspecified', fileprefix = ''):
    #function to update the value of a given and tracking column within the master location file
    #if old_value is specified, the operation will fail unless the existing value in the csv matches old_value
    #locGroup:  string (e.g: "79_Tigray_1")
    #loc:       string (e.g: "lon14_115lat38_4743")
    #column:    string (e.g: "00DownStatus")
    #old_value: string (e.g: "complete")
    #new_value: string (e.g: "initiated")
    
    with masterLocationFile_lock:
        df = pd.read_csv(f'{fileprefix}masterLocationFile{locGroup}.csv')
    
        #check if old_value is unspecified and matches the current value
        if old_value != 'unspecified':
            current_value = df.loc[df['Location'] == loc][column].item()
            if old_value != current_value:
                raise ValueError('old_value did not match the current value in the master location file. Could not update.')

        df.loc[df['Location'] == loc, column] = new_value
        df.to_csv(f'masterLocationFile{locGroup}.csv', index=False)
    
def locationFileSummary(locGroup, loc = 'unspecified', fileprefix = ''):
    #function to display the row associated with a loc in the master location file
    #if loc is unspecified, returns the entire location file as a dataframe
    #locGroup: string (e.g: "79_Tigray_1")
    #loc:      string (e.g: "lon14_115lat38_4743")
    
    with masterLocationFile_lock:        
        df = pd.read_csv(f'{fileprefix}masterLocationFile{locGroup}.csv')
        
        if loc == 'unspecified':
            return df
        else:
            return df[df["Location"] == loc]
