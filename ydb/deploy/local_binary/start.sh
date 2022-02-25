if [[ $1 != "disk" && $1 != "ram" ]]; then
  echo Please specify 'disk' or 'ram' as the parameter
  exit
fi
if [[ $1 = "disk" ]]; then
  if [ ! -f ydb.data ]; then
    echo Data file ydb.data not found, creating ...  
    fallocate -l 64G ydb.data
    if [[ $? -ge 1 ]]; then
      echo Error creating data file
      exit
    fi
  fi
  cfg=cfg_disk.yaml
else
  cfg=cfg_ram.yaml
fi
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:`pwd`/ydbd-master-linux-amd64/lib"
echo Starting storage process...
./ydbd-master-linux-amd64/bin/ydbd server --yaml-config ./$cfg --node 1 --grpc-port 2135 --ic-port 19001 --mon-port 8765 --log-file-name node_storage.log > node_storage_run.log 2>node_storage_err.log &
sleep 3
grep node_storage_err.log -v -f exclude_err.txt
if [[ $? -eq 0 ]]; then
  echo Errors found when starting storage process, cancelling start script
  exit
fi
echo Initializing storage ...
./ydbd-master-linux-amd64/bin/ydbd admin blobstorage config init --yaml-file ./$cfg > init_storage.log 2>&1
echo Registering database ...
./ydbd-master-linux-amd64/bin/ydbd admin database /Root/test create ssd:1 > database_create.log 2>&1
if [[ $? -ge 1 ]]; then
  echo Errors found when registering database, cancelling start script
  exit
fi
echo Starting database process...
./ydbd-master-linux-amd64/bin/ydbd server --yaml-config ./$cfg --tenant /Root/test --node-broker localhost:2135 --grpc-port 31001 --ic-port 31003 --mon-port 31002 --log-file-name node_db.log > node_db_run.log 2>node_db_err.log &
sleep 3
grep node_db_err.log -v -f exclude_err.txt
if [[ $? -eq 0 ]]; then
  echo Errors found when starting database process, cancelling start script
  exit
fi
echo Database started
