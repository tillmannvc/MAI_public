### VM creation
1. Create a google cloud project and in that a virtual machine with standard capabilities (2CPU, 4GB memory)
2. Create a permanent IP for the machine by going to [VPC Network > IP Addresses > Reserve External Static IP Address](https://console.cloud.google.com/networking/addresses/add?project=gcp-rural-economies) within the cloud console.
3. Name the IP address similarly to the VM, select "standard", "IPv4", "regional", set the zone to "us-west1 (Oregon)", attach the IP to to the VM, and click "reserve".

### Root user setup

1. Log in by clicking the machine's "SSH" button in the VM list.
2. Run `sudo sed -i 's/PermitRootLogin no/PermitRootLogin prohibit-password/g' /etc/ssh/sshd_config`
3. Restart the VM.
4. In cloudshell, run: `gcloud compute ssh --project=<PROJECT NAME> --zone=<ZONE> root@<VM-NAME>` (*important:* this will be the way you log into the VM from now on!)
5. `apt update && apt upgrade -y` to make sure everything is up to date

### Github setup

1. Set up a GitHub account, if you don't already have one.
2. In the VM, run `apt install git`.
3. Run `git config --global user.name "Your Name"` and `git config --global user.email you@example.com` to set your user identity.
4. Run `git clone <PATH TO THIS REPO>`

### Jupyter setup

1. `apt install jupyter-notebook`
2. `jupyter notebook --generate-config` to create a config file for Jupyter.
3. `nano .jupyter/jupyter_notebook_config.py` to edit the config file.
4. Enter these four lines into the config file:
```
c.NotebookApp.token = ''
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 2711
```
5. ctrl+o, then "enter" to save the file, and ctrl+x to exit the editor.
6. `jupyter-notebook --no-browser --port 2711 --allow-root` to start your notebook (this is how you will start notebooks from now on)
7. Go to `http://<VM-IP-ADDRESS>:2711/` to confirm Jupyter is working.

### Python setup

1. `cd ~ && apt install pip`
2. Run `pip install -r ./MAI2023/requirements.txt` to install python dependencies.

### Planet authentication

1. Run `planet auth init` to authenticate planet with your account login.

### Google Earth Engine authentication

1. Run `earthengine authenticate --quiet --force --auth_mode gcloud-legacy`
2. Copy the entire gcloud command it gives you (gcloud auth...), including the long URL, paste it into the command line tool on your own device (terminal for a Mac), and run. It should take you to a website where you can log in and authorize.
3. Complete the online authentication, then go back to your computer's command line and copy the url output (https://localhost...).
4. Paste this into your cloud shell that is logged into the VM, where it asks for the output of the above command.
5. Run the command `earthengine set_project planetupload` to establish which project you would like the earthengine command to work within.

### Google Earth Engine Git authentication
1. Go to https://earthengine.googlesource.com/new-password
2. Sign in, copy the bash script, paste into the cloud shell, and run.

### ee-runner installation
1. `curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash` to download Node Version Manager (NVM)
2. Run `export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"` and then `[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"` to load NVM
3. `nvm install stable` to get the latest version of nodejs
4. `npm install -g ee-runner@latest`
5. You can ensure it worked by testing `ee-runner -h`, and you can see install locations of node and ee-runner by using `which node` and `which ee-runner`.

### Running Code
1. `apt install tmux` to get tmux, a tool that helps manage terminal sessions
2. Use the command `tmux` to start a tmux session.
3. Start a jupyter session, open masterProcessor_alt.ipynb, and run all cells.

### MasterProcessor setup
1. Go into the masterProcessor_alt file, and update API key and locGroup


