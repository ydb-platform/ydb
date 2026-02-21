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
    YDB_DEFAULT_LOG_LEVEL=DEBUG ./local_cluster --binary-path ~/ydb_bg/ydb/apps/ydbd/ydbd
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

2) Create partition:
    ```
    cd ydb_bg/ydb/apps/dstool/
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition create --block-size 4096 --blocks-count 1000 --pool ddp1 --type=ssd
    ```

3) Write some data:
    ```
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition io --start_index 0 --type write --data "vnfjkdnsfvjdfknsjknsdkjnvnjk" --id "[1:7599782149963481987:2733]"
    ```

4) Read some data:
    ```
    ./ydb-dstool -d -e grpc://localhost:2135 nbs partition io --type read --blocks_count 1 --start_index 0 --id "[1:7600018021929343002:2699]"
    ```

3) Grep logs:
    ```
    cd /home/barkovbg/ydb_bg/ydb/tests/tools/local_cluster/.ydbd_working_dir/local_cluster
    cat node_*/logfile_* | grep "NBS_PARTITION" | sort
    ```

4) Run FIO from QEMU
    ```
    sudo fio --name=randomreadwritetest --blocksize=4096 --rw=randrw --direct=1 --buffered=0 --ioengine=libaio --iodepth=32 --runtime=30 --time_based --filename=/dev/vdb
    ```
