## Visual Studio Code

### Generate workspace

Run vscode_generate_workspace.sh to generate a workspace.
```
./vscode_generate_workspace.sh
```

### Open workspace

Execute
```
code workspace/workspace.code-workspace
```

## Run project locally

1) Firstly you need to build ydbd app.
    ```
    ya make ../../apps/ydbd
    ```

2) Then you can run project locally.
    ```
    tmux
    cd ../../tests/tools/local_cluster
    ya make
    YDB_DEFAULT_LOG_LEVEL=DEBUG ./local_cluster --binary-path ~/ydb_bg/ydb/apps/ydbd/ydbd --enable-nbs --port-offset 0
    ```
3) Forward the monitoring ports via SSH.

    You can find them in the startup script log.

    Example:
    ```
    2026-01-16 08:46:09,089 - __main__ - INFO - Cluster started successfully!
    2026-01-16 08:46:09,089 - __main__ - INFO - Total nodes: 9
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 1: GRPC=2135, MON=8765, IC=19001, Endpoint=localhost:2135
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 2: GRPC=2145, MON=8775, IC=19011, Endpoint=localhost:2145
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 3: GRPC=2155, MON=8785, IC=19021, Endpoint=localhost:2155
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 4: GRPC=2165, MON=8795, IC=19031, Endpoint=localhost:2165
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 5: GRPC=2175, MON=8805, IC=19041, Endpoint=localhost:2175
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 6: GRPC=2185, MON=8815, IC=19051, Endpoint=localhost:2185
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 7: GRPC=2195, MON=8825, IC=19061, Endpoint=localhost:2195
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 8: GRPC=2205, MON=8835, IC=19071, Endpoint=localhost:2205
    2026-01-16 08:46:09,089 - __main__ - INFO - Node 9: GRPC=2215, MON=8845, IC=19081, Endpoint=localhost:2215
    ```

    You only need to forward one of the MON ports:
    ```
    в ssh -L [LOCAL_PORT]:[DESTINATION_ADDRESS]:[DESTINATION_PORT] [USER@]SSH_SERVER
    ```
    For example:
    ```
    ssh -L 8765:localhost:8765 cloud
    ```
    Alternatively, you can forward ports directly through VS Code.

## IO
1) Setup ddisks:
    ```
    ./ydbd admin bs config invoke --proto 'Command { DefineDDiskPool { BoxId: 1 Name: "ddp1" Geometry { NumFailRealms: 1 NumFailDomainsPerFailRealm: 5 NumVDisksPerFailDomain: 1 RealmLevelBegin: 10 RealmLevelEnd: 10 DomainLevelBegin: 10 DomainLevelEnd: 40 } PDiskFilter { Property { Type: ROT } } NumDDiskGroups: 10 } }'
    ```

  On remote:
    ```
    ./ydbd --server "vla5-8226.search.yandex.net" admin bs config invoke --proto 'Command { DefineDDiskPool { BoxId: 1 Name: "ddp1" Geometry { NumFailRealms: 1 RealmLevelBegin: 10 RealmLevelEnd: 10 DomainLevelBegin: 10 DomainLevelEnd: 40 NumFailDomainsPerFailRealm: 5 NumVDisksPerFailDomain: 1} PDiskFilter { Property { Type: SSD } } NumDDiskGroups: 3 } }'
    ```

2) Create partition:
    ```
    cd ydb_bg/ydb/apps/dstool/
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition create --block-size 4096 --blocks-count 32768 --pool ddp1 --type=ssd --disk-id disk1
    ```
    On Remote:
    ```
    cd ydb_bg/ydb/apps/dstool/
    ./ydb-dstool -d -e grpc://vla5-8296.search.yandex.net:2135 nbs partition create --block-size 4096 --blocks-count 32768 --pool ddp1 --type=ssd --disk-id disk1
    ```

3) Get load actor adapter id:
    ```
    cd ydb_bg/ydb/apps/dstool/
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition get-load-actor-adapter-actor-id --disk-id disk1
    ```

4) Write some data:
    ```
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition io --start_index 0 --type write --data "vnfjkdnsfvjdfknsjknsdkjnvnjk" --id "[1:7599782149963481987:2733]"
    ```

5) Read some data:
    ```
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition io --type read --blocks_count 1 --start_index 0 --id "[1:7600018021929343002:2699]"
    ```

6) Grep logs:
    ```
    cd /home/barkovbg/ydb_bg/ydb/tests/tools/local_cluster/.ydbd_working_dir/local_cluster
    cat node_*/logfile_* | grep "NBS_PARTITION" | sort
    ```

7) Run FIO from QEMU:
    ```
    sudo fio --name=randomreadwritetest --blocksize=4096 --rw=randrw --direct=1 --buffered=0 --ioengine=libaio --iodepth=32 --runtime=30 --time_based --filename=/dev/vdb

    sudo fio --rw=randwrite --name=test --filename=/dev/vdb --direct=1 --blocksize=4096 --ioengine=libaio --iodepth=32 --runtime=30 --numjobs=1 --time_based --group_reporting --verify_fatal=1 --verify_dump=1 --verify_async=2 --do_verify=1 --verify=sha1 --verify_backlog=500
    ```

## Slice deployment

1) Ensure that you forward skotty default socket to your remote machine.
In `.ssh/config`:
```
Host cloud
  HostName <ipv6>
  IdentityFile <your_key>
  ForwardAgent ~/.skotty/sock/default.sock
```

2) Build on your remote dev machine:
```
ya make ydb/apps/ydbd
```

3) Build ydbd_slice on your remote dev machine:
```
ya make ydb/tools/ydbd_slice/bin
```

4) Create cluster config (but with your own server fqdn's)
```
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  erasure: block-4-2
  fail_domain_type: disk
  self_management_config:
    enabled: true
  default_disk_type: SSD
  host_configs:
  - drive:
    - path: /dev/disk/by-partlabel/kikimr_nvme_01
      type: SSD
    - path: /dev/disk/by-partlabel/kikimr_nvme_02
      type: SSD
    - path: /dev/disk/by-partlabel/kikimr_nvme_03
      type: SSD
    - path: /dev/disk/by-partlabel/kikimr_nvme_04
      type: SSD
    host_config_id: 1
  hosts:
    - host: vla5-8268.search.yandex.net
      host_config_id: 1
      location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
    - host: vla5-8186.search.yandex.net
      host_config_id: 1
      location:
        body: 2
        data_center: 'zone-a'
        rack: '2'
    - host: vla5-8236.search.yandex.net
      host_config_id: 1
      location:
        body: 3
        data_center: 'zone-a'
        rack: '3'
    - host: vla5-8262.search.yandex.net
      host_config_id: 1
      location:
        body: 4
        data_center: 'zone-a'
        rack: '4'
    - host: vla5-8226.search.yandex.net
      host_config_id: 1
      location:
        body: 5
        data_center: 'zone-a'
        rack: '5'
    - host: vla5-8253.search.yandex.net
      host_config_id: 1
      location:
        body: 6
        data_center: 'zone-a'
        rack: '6'
    - host: vla5-8258.search.yandex.net
      host_config_id: 1
      location:
        body: 7
        data_center: 'zone-a'
        rack: '7'
    - host: vla5-8250.search.yandex.net
      host_config_id: 1
      location:
        body: 8
        data_center: 'zone-a'
        rack: '8'
  interconnect_config:
    start_tcp: true
  grpc_config:
    services_enabled:
    - legacy
  nbs_config:
    enabled: true
    nbs_storage_config:
      scheme_shard_dir: /Root/NBS
      folder_id: "testFolder"
      ssd_system_channel_pool_kind: "ssd"
      ssd_log_channel_pool_kind: "ssd"
      ssd_index_channel_pool_kind: "ssd"
      pipe_client_retry_count: 3
      pipe_client_min_retry_time: 1
      pipe_client_max_retry_time: 10
  log_config:
    entry:
      - component: NBS_PARTITION
        level: 7
```

5) Add databases config
```
domains:
   - domain_name: Root
     dynamic_slots: 8
     databases:
       - name: "NBS"
         storage_units:
           - count: 1
             kind: ssd
         compute_units:
           - count: 1
             kind: slot
             zone: any
```

6) Run ydbd_slice
```
ydb/tools/ydbd_slice/bin/ydbd_slice install <path_to_databases_config.yaml> all --yaml-config <path_to_cluster_config.yaml> --binary <path_to_ydbd_binary>
```

7) Now you can work with your own slice.
- Monitoring is available on `<fqdn>:8765/`
- GRPC is available on `<fqdn>:2135/`

## Tracing setup

1) Add tracing config to ydb configuration:
- `ydb/tests/library/harness/resources/default_yaml.yml` for local_cluster configuration
- `config.yaml` for slice configuration
```
tracing_config:
   backend:
     opentelemetry:
       collector_url: grpc://localhost:4316
       service_name: barkovbg
```
Object tracing_config has to be a child to "config"

2) There is no other configuration for a slice.
Address: https://monitoring.yandex-team.ru/projects/kikimr/traces.
Add filter "service=barkovbg" - where service is service_name from the config.

3) The following instructions apply only to local development.

4) Install unified agent

5) Create unified agent config on the path `/etc/yandex/unified_agent/conf.d`:
```
routes:
  - input:
      id: kikimr-tracing-input
      plugin: otel_traces
      config:
        protocol: grpc
        uri: localhost:4316
    channel:
      output:
        id: kikimr-tracing-output
        plugin: otel_traces
        config:
          url: "collector.tracing.cloud-preprod.yandex.net:4317"
          format: proto
          batch:
            flush_period: 5s
          message_quota: 100000
          set_host_label: short_host_name
          project: "kikimr"
          cluster: !expr "{$file('/Berkanavt/kikimr/cfg/cluster.txt')|ydb-other}"
```

6) Restart unified agent `sudo service unified-agent status`

7) View traces in Cloud Monitoring. Example URL:
```
https://monitoring-preprod.yandex.cloud/projects/aoedo0ji1lgce9l91har/traces?from=now-1h&to=now&refresh=60000&query=%7Bproject+%3D+%22kikimr%22%2C+service+%3D+%22barkovbg%22%7D
```

## Run qemu on a slice

1) Build the image from the nbs repo: `nbs/cloud/storage/core/tools/testing/qemu/image`

2) Build the qemu tar from the nbs repo: `cloud/storage/core/tools/testing/qemu/bin`

3) Copy the image and qemu tar to one of the slice hosts:
```
scp rootfs.img vla5-8296.search.yandex.net:/tmp/.
scp qemu-bin.tar.gz vla5-8296.search.yandex.net:~/.
```

4) Move rootfs.img from `/tmp` to your home directory:
```
sudo mv /tmp/rootfs.img .
```

5) SSH into one of the slice hosts and create the run_qemu.sh script with the following content:
```
#!/usr/bin/env bash

find_bin_dir() {
    readlink -e `dirname $0`
}

BIN_DIR=`find_bin_dir`
QEMU_TAR="."

function qemu_bin_dir {
    echo $(dirname $QEMU_TAR)
}

show_help() {
    cat << EOF
Usage: ./7-run_qemu.sh [-hkds]
Run qemu
-h, --help                     Display help
-d, --diskid                   NBS Disk ID
-s, --socket                   Socket path
-k, --encryption-key-path      Encryption key path
-e, --encrypted                Use default encryption key
EOF
}

#defaults
encryption=""
diskid=""
socket=""
options=$(getopt -l "help,key:,diskid:,socket:,encryption-key-path:,encrypted" -o "hk:d:s:e" -a -- "$@")

if [ $? != 0 ] ; then
    echo "Incorrect options provided"
    exit 1
fi
eval set -- "$options"

while true
do
    case "$1" in
    -h | --help )
        show_help
        exit 0
        ;;
    -k | --encryption-key-path )
        encryption="--encryption-mode=aes-xts --encryption-key-path=${2}"
        shift 2
        ;;
    -e | --encrypted )
        encryption="--encryption-mode=aes-xts --encryption-key-path=encryption-key.txt"
        shift 1
        ;;
    -d | --diskid )
        diskid=${2}
        shift 2
        ;;
    -s | --socket )
        socket=${2}
        shift 2
        ;;
    --)
        shift
        break;;
    esac
done

if [ -z "$diskid" ] ; then
    echo "Disk id shouldn't be empty"
    exit 1
fi

if [ -z "$socket" ] ; then
    socket="/tmp/$diskid.sock"
fi

# prepare qemu image

QEMU_BIN_DIR=$BIN_DIR/$(qemu_bin_dir)
QEMU_BIN_TAR=$QEMU_BIN_DIR/qemu-bin.tar.gz
QEMU=$QEMU_BIN_DIR/usr/bin/qemu-system-x86_64
QEMU_FIRMWARE=$QEMU_BIN_DIR/usr/share/qemu
DISK_IMAGE=rootfs.img

[[ ( ! -x $QEMU ) ]] &&
      echo expand qemu tar from [$QEMU_BIN_TAR]
      tar -xzf $QEMU_BIN_TAR -C $QEMU_BIN_DIR

# start endpoint for disk
#echo "starting endpoint [${socket}] for disk [${diskid}]"
#blockstore-client stopendpoint --socket $socket
#blockstore-client startendpoint --ipc-type vhost --socket $socket --client-id client-1 --instance-id localhost --disk-id $diskid --persistent $encryption
#sleep 1

# run qemu with secondary disk
qmp_port=8678
ssh_port=8679

MACHINE_ARGS=" \
    -L $QEMU_FIRMWARE \
    -snapshot \
    -nodefaults
    -cpu host \
    -smp 4,sockets=1,cores=4,threads=1 \
    -enable-kvm \
    -m 16G \
    -name debug-threads=on \
    -qmp tcp:127.0.0.1:${qmp_port},server,nowait \
    "

MEMORY_ARGS=" \
    -object memory-backend-memfd,id=mem,size=16G,share=on \
    -numa node,memdev=mem \
    "

NET_ARGS=" \
    -netdev user,id=netdev0,hostfwd=tcp::${ssh_port}-:22 \
    -device virtio-net-pci,netdev=netdev0,id=net0 \
    "

DISK_ARGS=" \
    -object iothread,id=iot0 \
    -drive format=qcow2,file=$DISK_IMAGE,id=lbs0,if=none,aio=native,cache=none,discard=unmap \
    -device virtio-blk-pci,scsi=off,drive=lbs0,id=virtio-disk0,iothread=iot0,bootindex=1 \
    "

NBS_ARGS=" \
    -chardev socket,id=vhost0,path=$socket \
    -device vhost-user-blk-pci,chardev=vhost0,id=vhost-user-blk0,num-queues=1 \
    "

echo "Running qemu with disk [$diskid]"
$QEMU \
    $MACHINE_ARGS \
    $MEMORY_ARGS \
    $NET_ARGS \
    $DISK_ARGS \
    $NBS_ARGS \
    -nographic \
    -serial stdio \
    -s
```

6) Make this script executable:
```
chmod +x run_qemu.sh
```

7) Console fix after qemu exits:
```
printf '\e[?7h'
```

8) Now you can run qemu from any of the slice hosts:
```
sudo ./run_qemu.sh -d disk1
```

## Ya configuration

Ya configuration exists on the path `~/.ya/ya.conf`

1) Configuration without sanitizers
```
tools_cache_size = "6GiB"
symlinks_ttl = "30d"
cache_size = "500GiB"
print_statistics = false
build_cache = false
link_threads = 4
continue_on_fail = false
content_uids = true

[flags]
#SANITIZER_TYPE="address"
#SANITIZER_TYPE="thread"

[[target_platform]]
platform_name = "default-linux-x86_64"
build_type = "relwithdebinfo"
#build_type = "release"

[target_platform.flags]
FORCE_STATIC_LINKING="yes"
#SKIP_JUNK="yes"
#USE_EAT_MY_DATA="yes"
#DEBUGINFO_LINES_ONLY="yes"
```

2) Configuration with asan

```
tools_cache_size = "6GiB"
symlinks_ttl = "30d"
cache_size = "500GiB"
print_statistics = false
build_cache = false
link_threads = 4
continue_on_fail = false
content_uids = true

[flags]
SANITIZER_TYPE="address"
#SANITIZER_TYPE="thread"

[[target_platform]]
platform_name = "default-linux-x86_64"
build_type = "relwithdebinfo"
#build_type = "release"

[target_platform.flags]
FORCE_STATIC_LINKING="yes"
SKIP_JUNK="yes"
USE_EAT_MY_DATA="yes"
DEBUGINFO_LINES_ONLY="yes"
```

Actually you need to add environment variable to make asan output readable

```
export ASAN_SYMBOLIZER_PATH=$(ya tool llvm-symbolizer --print-path)
```

3) Configuration with tsan

```
tools_cache_size = "6GiB"
symlinks_ttl = "30d"
cache_size = "500GiB"
print_statistics = false
build_cache = false
link_threads = 4
continue_on_fail = false
content_uids = true

[flags]
#SANITIZER_TYPE="address"
SANITIZER_TYPE="thread"

[[target_platform]]
platform_name = "default-linux-x86_64"
build_type = "relwithdebinfo"
#build_type = "release"

[target_platform.flags]
FORCE_STATIC_LINKING="yes"
SKIP_JUNK="yes"
USE_EAT_MY_DATA="yes"
DEBUGINFO_LINES_ONLY="yes"
```
