#!/usr/bin/env bash

NODE=${NODE:-1}

function parse_args {
    # Default action is 'start' if no argument is provided
    ACTION=${1:-start}

    # Default ports
    GRPC_PORT=${GRPC_PORT:-9001}
    MON_PORT=${MON_PORT:-8765}

    # Parse named arguments
    shift  # Remove the first argument (action)

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --port)
                if [ -n "$2" ]; then
                    # Check if the port is an integer
                    if ! [[ "$2" =~ ^[0-9]+$ ]]; then
                        echo "Error: Port must be an integer"
                        echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
                        exit 1
                    fi
                    GRPC_PORT=$2
                    shift 2
                else
                    echo "Error: --port requires a value"
                    echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
                    exit 1
                fi
                ;;
            --mon-port)
                if [ -n "$2" ]; then
                    # Check if the port is an integer
                    if ! [[ "$2" =~ ^[0-9]+$ ]]; then
                        echo "Error: Monitor port must be an integer"
                        echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
                        exit 1
                    fi
                    MON_PORT=$2
                    shift 2
                else
                    echo "Error: --mon-port requires a value"
                    echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
                    exit 1
                fi
                ;;
            *)
                echo "Error: Unknown argument $1"
                echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
                exit 1
                ;;
        esac
    done
}

# Parse command line arguments
parse_args "$@"

YDBD_BIN="`pwd`/../../../../ydb/apps/ydbd/ydbd"
DSTOOL_BIN="`pwd`/../../../../ydb/apps/dstool/ydb-dstool"

for bin in $YDBD_BIN
do
  if ! test -f $bin; then
    echo "$bin not found, build all targets first"
    exit 1
  fi
done

function dstool {
  $DSTOOL_BIN "$@"
}

function ydbd {
  LD_LIBRARY_PATH=$(dirname $YDBD_BIN) $YDBD_BIN "$@"
}

function stop_ydbd {
    echo "Stop ydbd"
    ps aux | grep "$YDBD_BIN server" | grep -v "grep" | awk '{print $2}' | while read line;do kill -9 $line;done
    sleep 3
}

function start_ydbd {
    DATA_DIRS=" \
        data \
        logs \
        "

    find_bin_dir() {
        readlink -e `dirname $0`
    }

    BIN_DIR=`find_bin_dir`
    PERSISTENT_TMP_DIR=${PERSISTENT_TMP_DIR:-$HOME/tmp/nbs}

    stop_ydbd

    echo "Data and logs folders:"
    for dir in $DATA_DIRS; do
        mkdir -p "$PERSISTENT_TMP_DIR/$dir"
        ln -svfT "$PERSISTENT_TMP_DIR/$dir" "$BIN_DIR/$dir"
    done

    find_bin_dir() {
        readlink -e `dirname $0`
    }

    format_disk() {
        DISK_GUID=$1
        DISK_SIZE=$2

        DISK_FILE=data/pdisk-${DISK_GUID}.data

        rm -f $DISK_FILE
        dd if=/dev/zero of=$DISK_FILE bs=1 count=0 seek=$DISK_SIZE > /dev/null 2>&1 &
    }


    format_disk ssd-1 64G
    format_disk ssd-2 64G
    format_disk rot 32G

    echo "Starting ydbd"
    ydbd server \
        --tcp \
        --node              $NODE \
        --grpc-port         $GRPC_PORT \
        --mon-port          $MON_PORT \
        --yaml-config       static/config.yaml \
        --suppress-version-check > $PERSISTENT_TMP_DIR/logs/ydbd.log 2>&1 &

    sleep 3

    DATA_DIR="data"

    echo "DefineBox"
    ydbd -s grpc://localhost:$GRPC_PORT admin bs config invoke --proto-file dynamic/DefineBox.txt
    echo "DefineStoragePools"
    ydbd -s grpc://localhost:$GRPC_PORT admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
    echo "BindRootStorageRequest-Root"
    ydbd -s grpc://localhost:$GRPC_PORT db schema execute dynamic/BindRootStorageRequest-Root.txt
    echo "CreateTenant"
    ydbd -s grpc://localhost:$GRPC_PORT admin console execute --domain=Root --retry=10 dynamic/CreateTenant.txt
    echo "Configure-Root"
    ydbd -s grpc://localhost:$GRPC_PORT admin console execute --domain=Root --retry=10 dynamic/Configure-Root.txt

    ALLOW_NAMED_CONFIGS_REQ="
ConfigsConfig {
    UsageScopeRestrictions {
        AllowedTenantUsageScopeKinds: 100
        AllowedHostUsageScopeKinds:   100
        AllowedNodeTypeUsageScopeKinds: 100
    }
}
"

    echo "AllowNamedConfigs"
    ydbd -s grpc://localhost:$GRPC_PORT admin console config set --merge "$ALLOW_NAMED_CONFIGS_REQ"

    echo "Set NBS_PARTITION log level to debug"
    DDISK_LOG_CONFIG_REQ="
    ConfigureRequest {
        Actions {
            AddConfigItem {
                ConfigItem {
                    Kind: 2
                    Config {
                        LogConfig {
                            DefaultLevel: 3
                            Entry {
                                Component: \"NBS_PARTITION\"
                                Level: 7
                            }
                        }
                    }
                }
            }
        }
    }
    "
    TEMP_FILE=$(mktemp)
    echo "$DDISK_LOG_CONFIG_REQ" > "$TEMP_FILE"
    ydbd -s grpc://localhost:$GRPC_PORT admin console execute --domain=Root --retry=10 "$TEMP_FILE"
    rm -f "$TEMP_FILE"

    printf "\n\nYdbd monitoring is running at $MON_PORT, logs in $PERSISTENT_TMP_DIR/logs/ydbd.log\n"
}

function create_ddisk_pool {
    echo ""
    echo "Create ddisk pool"
    ydbd --server localhost:$GRPC_PORT admin bs config invoke \
        --proto 'Command { DefineDDiskPool
            { BoxId: 1 Name: "ddp1" Geometry
            { NumFailRealms: 1 NumFailDomainsPerFailRealm: 1
            NumVDisksPerFailDomain: 1 RealmLevelBegin: 10
            RealmLevelEnd: 10 DomainLevelBegin: 10 DomainLevelEnd: 40 }
            PDiskFilter { Property { Type: SSD } } NumDDiskGroups: 10 } }'
}

function create_partition {
    echo ""
    echo "Create partition"
    dstool --endpoint grpc://localhost:$GRPC_PORT nbs partition create --pool ddp1
}


# Handle different actions
case "$ACTION" in
    stop)
        stop_ydbd
        ;;
    start)
        start_ydbd
        create_ddisk_pool
        create_partition
        ;;
    *)
        echo "Usage: $0 [start|stop] [--port PORT] [--mon-port PORT]"
        echo "  start [--port PORT] [--mon-port PORT] - Stop any existing ydbd process and start a new one (default)"
        echo "  stop                                    - Stop any existing ydbd process and exit"
        echo "  --port PORT                             - Specify GRPC port number (default: 9001)"
        echo "  --mon-port PORT                         - Specify monitoring port number (default: 8765)"
        exit 1
        ;;
esac
