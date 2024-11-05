#!/usr/bin/env bash
# set -v
YDBD_PATH=${YDBD_PATH:-`pwd`/ydbd/bin/ydbd}
YDBD_LIB_PATH=${YDBD_LIB_PATH:-`pwd`/ydbd/lib}
BASE_PATH=$(dirname -- "${BASH_SOURCE[0]}")
CONFIG_PATH="${BASE_PATH}/config"
LOGS_PATH="${BASE_PATH}/logs"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$YDBD_LIB_PATH"

if [[ $1 != "drive" && $1 != "disk" && $1 != "ram" ]]; then
  echo Please specify 'drive', 'disk' or 'ram' as the first parameter
  exit
fi
need_init=0
if [[ $1 = "drive" ]]; then
  if [[ -z "$2" ]]; then
    echo Drive mode needs an extra parameter with a path to disk device
    exit
  fi
  disk="$2"
  cfg_suffix=$(echo $2 | sed -e 's|\/|_|g' -e 's|\s+|_|g')
  cfg="drive$cfg_suffix.yaml"
  if [[ ! -f "$CONFIG_PATH/$cfg" ]]; then # if we already have the config, then we have already prepared the drive and won't wipe it again
    read -p "Drive $disk is going to be fully wiped. All data it contains will be lost. Enter (yes) or (y) if you want to continue: " safeguard
    case $safeguard in
      [Yy]* ) echo "You have confirmed, proceeding.";;
      * ) echo "You have chosen not to continue, aborting."; exit 1;
    esac
    disk_by_partlabel="/dev/disk/by-partlabel/$cfg_suffix"
    cat "$CONFIG_PATH/drive.yaml" | sed -e "s|DRIVE_PATH_PLACEHOLDER|$disk_by_partlabel|g" > "$CONFIG_PATH/$cfg"
    sudo parted "$disk" mklabel gpt -s
    sudo parted -a optimal "$disk" mkpart primary 0% 100% 2>/dev/null
    sudo parted "$disk" name 1 "$cfg_suffix"
    sudo partx --u "$disk"
    sleep 1 # chown fails otherwise
    sudo chown "$(whoami)" "$disk_by_partlabel"
    $YDBD_PATH admin bs disk obliterate "$disk_by_partlabel"
    need_init=1
  fi
elif [[ $1 = "disk" ]]; then
  if [ ! -f ydb.data ]; then
    echo Data file ydb.data not found, creating ...
    fallocate -l 80G ydb.data
    if [[ $? -ge 1 ]]; then
      echo fallocate failed. Proably not supported by FS, trying to use dd ...
      dd if=/dev/zero of=ydb.data bs=1G count=0 seek=80
      if [[ $? -ge 1 ]]; then
        if [ -f ydb.data ]; then
          rm ydb.data
        fi
        echo Error creating data file
        exit
      fi
    fi
    need_init=1
  fi
  cfg=disk.yaml
else
  cfg=ram.yaml
fi
echo Starting storage process... takes ~10 seconds
mkdir -p "$LOGS_PATH"
$YDBD_PATH server --yaml-config "$CONFIG_PATH/$cfg" --node 1 \
  --log-file-name "$LOGS_PATH/storage_start.log" > "$LOGS_PATH/storage_start_output.log" 2>"$LOGS_PATH/storage_start_err.log" &
sleep 10
grep "$LOGS_PATH/storage_start_err.log" -v -f "$CONFIG_PATH/exclude_err.txt"
if [[ $? -eq 0 ]]; then
  echo Errors found when starting storage process, cancelling start script
  if [ $need_init -eq 1 ] && [ "$cfg" = "disk.yaml" ]; then
    rm ydb.data
  fi
  exit
fi
if [ $need_init -eq 1 ] || [ "$cfg" = "ram.yaml" ]; then
  echo Initializing storage...
  $YDBD_PATH -s grpc://localhost:2136 admin blobstorage config init --yaml-file "$CONFIG_PATH/$cfg" > "$LOGS_PATH/init_storage.log" 2>&1
  if [[ $? -ge 1 ]]; then
    echo Errors found when initializing storage, cancelling start script, check logs/init_storage.log
    if [ $need_init -eq 1 ]; then
	    rm ydb.data
    fi
    exit
  fi
fi
echo Registering database...
$YDBD_PATH -s grpc://localhost:2136 admin database /Root/test create ssd:1 > "$LOGS_PATH/db_reg.log" 2>&1
if [[ $? -ge 1 ]]; then
  echo Errors found when registering database, cancelling start script, check "$LOGS_PATH/db_reg.log"
  exit
fi
echo Starting database process...
$YDBD_PATH server --yaml-config "$CONFIG_PATH/$cfg" --tenant /Root/test --node-broker localhost:2136 --grpc-port 31001 --ic-port 31003 --mon-port 31002 \
  --log-file-name "$LOGS_PATH/db_start.log" > "$LOGS_PATH/db_start_output.log" 2>"$LOGS_PATH/db_start_err.log" &
sleep 3
grep "$LOGS_PATH/db_start_err.log" -v -f "$CONFIG_PATH/exclude_err.txt"
if [[ $? -eq 0 ]]; then
  echo Errors found when starting database process, cancelling start script
  exit
fi
echo "
Database started. Connection options for YDB CLI:

-e grpc://localhost:2136 -d /Root/test
"
