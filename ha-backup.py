import argparse
from datetime import datetime
import os
import hashlib
import time
from homeassistant_api import Client
from paramiko import SSHClient, SFTPClient, AutoAddPolicy

def path_contains_ha_config(sftp, path):
    return "configuration.yaml" in sftp.listdir(path)

def backups_folder_exists(ssh, path):
    return len(ssh.exec_command(f"ls {path} | grep backups")[1].readlines()) > 0

def get_backups_filenames(ssh, path):
    return [filename.strip() for filename in ssh.exec_command(f"ls {path}")[1].readlines()]

def get_backups_count(ssh, path):
    return len(get_backups_filenames(ssh, path))

def delete_backups(ssh, path, path_backups):
    if backups_folder_exists(ssh, path) and get_backups_count(ssh, path_backups) > 0:
        print("Removing existing backups...")
        for backup_filename in get_backups_filenames(ssh, path_backups):
            absolute_path = os.path.join(path_backups, backup_filename)
            if not absolute_path.endswith(".tar"):
                print("Trying to remove a file without *.tar extension, skipping")
            else:
                print(f"Removing {absolute_path}")
                ssh.exec_command(f"sudo rm -f {absolute_path}")

if __name__ != "__main__":
    quit()

parser = argparse.ArgumentParser("HomeAssistant backup", "Creates backups for HomeAssistant and stores them locally.")
parser.add_argument("url", metavar="<url>", type=str, help="Home Assistant URL")
parser.add_argument("path", metavar="<source path>", type=str, help="Path to HA's config folder")
parser.add_argument("local_path", metavar="<destination path>", type=str, help="Path to destination folder")
parser.add_argument("token", metavar="<token>", type=str, help="Long-Lived Access Token of HA's admin account")

args = parser.parse_args()
token = args.token
url = args.url + ("api" if args.url.endswith("/") else "/api")

source_path = args.path + ("/" if not args.path.endswith("/") else "")
source_path_backups = os.path.join(source_path, "backups/")

local_path = args.local_path + ("/" if not args.local_path.endswith("/") else "")
if not os.path.exists(local_path):
    print("Destination path does not exist")
    quit()

print(f"Connecting to source SSH on {url}")
ssh = None
sftp = None
try:
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy)
    ssh.connect("192.168.0.4", username="bosek", password="xj8h85WS")
    sftp = SFTPClient.from_transport(ssh.get_transport())
    print("Connected to SSH")
except:
    print("Connection failed")
    quit()

print("Checking if path to Home Assistant is a valid config folder")
if not path_contains_ha_config(sftp, source_path):
    print("Path to Home Assistant config folder is probably not a valid config folder, no configuration.yaml found")
    quit()
delete_backups(ssh, source_path, source_path_backups)

print(f"Connecting to Home Assistant on {url}")
client = None
try:
    client = Client(url, token)
    is_running = client.check_api_running()
    assert is_running
    print("Connected to Home Assistant")
except:
    print("Connection failed")
    quit()

print("Triggering Home Assistant backup")
client.trigger_service("backup", "create")

print("Checking if backup file was created")
backup_exists = False
for i in range(1,6):
    if get_backups_count(ssh, source_path_backups) == 0:
        print(f"Not yet, waiting one minute({i}/5)")
        time.sleep(60)
    else:
        print("Backup file was created")
        backup_exists = True
        break
if not backup_exists:
    print("Backup file was NOT created")
    quit()

for backup_filename in get_backups_filenames(ssh, source_path_backups):
    source_path_backup_absolute = os.path.join(source_path_backups, backup_filename)

    print("Waiting till backup is fully created")
    source_last_fsize = None
    backup_created = False
    for i in range(1,121):
        with sftp.open(source_path_backup_absolute, "rb") as f:
            fsize = f.stat().st_size
            if fsize > 0 and fsize == source_last_fsize:
                backup_created = True
                print("Backup is fully created")
                break
            else:
                source_last_fsize = fsize
                print(f"Not yet, waiting one minute({i}/120)")
        time.sleep(60)
    if not backup_created:
        print("Backup creation takes too much time")
        quit()
    
    source_md5_sum = ssh.exec_command(f"md5sum -b {source_path_backup_absolute}")[1].readlines()[0].strip().split(" ")[0]
    local_md5_sum = None
    
    with sftp.open(source_path_backup_absolute, "rb") as f:
        f.prefetch()
        fstat = f.stat()

        fsize = fstat.st_size
        mtimestamp = datetime.fromtimestamp(fstat.st_mtime).strftime("%Y%m%d%H%M%S")
        local_name = os.path.join(local_path, mtimestamp + os.path.splitext(backup_filename)[1])
        md5_hash = hashlib.md5()

        print(f"Transfering {local_name}")
        print("0%", end="\r")
        downloaded_bits = 0
        with open(local_name, "wb") as f2:
            while data:=f.read(2048):
                f2.write(data)
                f2.flush()
                md5_hash.update(data)

                downloaded_bits = downloaded_bits + 2048
                print(f"{downloaded_bits/fsize*100:.2f}%", end="\r")
        print("")
        local_md5_sum = md5_hash.hexdigest()

    print(f"MD5 sum is {('matching' if source_md5_sum == local_md5_sum else 'NOT matching')}")
    if source_md5_sum != source_md5_sum:
        print(f"Deleting {local_name}")
        os.remove(local_name)
        quit()
    print("Backup transferred")
    #delete_backups(ssh, source_path, source_path_backups)

entities = client.get_entities()
if "input_datetime" in entities.keys():
    entities_input_datetime = entities["input_datetime"].entities
    if "backup_last" in entities_input_datetime.keys():
        print("Updating input_datetime.backup_last entity state")
        entity_backup_last = entities_input_datetime["backup_last"]
        entity_backup_last.state.state = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        entity_backup_last.update_state()

sftp.close()
ssh.close()
