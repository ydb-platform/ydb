#!/bin/sh -e

#
# Usage: ./uig.sh PORT [--gdb]
#
# OPTIONS:
#     --gdb   run under gdb
#

SCRIPT_DIR="$(dirname $(readlink -f "$0"))"
UDFS_DIR="${SCRIPT_DIR}/../../udfs"
if [ -d "${SCRIPT_DIR}/../../../../../yql" ]; then
UDFS_DIR="${UDFS_DIR};${SCRIPT_DIR}/../../../../../yql/udfs"
fi
if [ -d "${SCRIPT_DIR}/../../../../../yql" ]; then
PG_EXT_OPT="--pg-ext pg_ext.txt"
else
PG_EXT_OPT=
fi

ASSETS_DIR=${SCRIPT_DIR}/http/www
MOUNTS_CFG=${SCRIPT_DIR}/mounts.txt
GATEWAYS_CFG=${SCRIPT_DIR}/../../cfg/tests/gateways.conf

PORT=${1:-3000}

if [ "$2" = "--gdb" ]; then
    GDB="yag tool gdb --args"
fi

${GDB} ${SCRIPT_DIR}/yqlrun ui \
    --mounts ${MOUNTS_CFG} \
    --udfs-dir ${UDFS_DIR} \
    --assets ${ASSETS_DIR} \
    --gateways-cfg ${GATEWAYS_CFG} \
    --remote --port $PORT \
    $PG_EXT_OPT
