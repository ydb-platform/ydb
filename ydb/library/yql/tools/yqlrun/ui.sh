#!/bin/sh -e

#
# Usage: ./ui.sh PORT [--gdb]
#
# OPTIONS:
#     --gdb   run under gdb
#

SCRIPT_DIR="$(dirname $(readlink -f "$0"))"
cd $SCRIPT_DIR/../../../../..
ROOT_DIR=$PWD
cd -

ASSETS_DIR="${SCRIPT_DIR}/http/www"
MOUNTS_CFG="${SCRIPT_DIR}/mounts.txt"
UDFS_DIR="${ROOT_DIR}/ydb/library/yql/udfs"
GATEWAYS_CFG="${ROOT_DIR}/ydb/library/yql/cfg/tests/gateways.conf"

PORT=${1:-3000}

YA="$ROOT_DIR/ya"
if [ ! -x "$YA" ];  then
    YA=ya
fi

if [ "$2" = "--gdb" ]; then
    GDB="$YA tool gdb --args"
fi

${GDB} ${SCRIPT_DIR}/yqlrun ui \
    --mounts ${MOUNTS_CFG} \
    --udfs-dir ${UDFS_DIR} \
    --assets ${ASSETS_DIR} \
    --gateways-cfg ${GATEWAYS_CFG} \
    --remote --port $PORT
