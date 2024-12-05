#!/bin/sh -e

#
# Usage: ./uig.sh PORT [--gdb]
#
# OPTIONS:
#     --gdb   run under gdb
#

SCRIPT_DIR="$(dirname $(readlink -f "$0"))"

UDFS_DIR="${SCRIPT_DIR}/../../essentials/udfs;${SCRIPT_DIR}/../../../ydb/library/yql/udfs"
if [ -d "${SCRIPT_DIR}/../../udfs" ]; then
UDFS_DIR="${UDFS_DIR};${SCRIPT_DIR}/../../udfs"
fi
if [ -d "${SCRIPT_DIR}/../../../../../yql/pg_ext" ]; then
PG_EXT_OPT="--pg-ext pg_ext.txt"
else
PG_EXT_OPT=
fi

ASSETS_DIR=${SCRIPT_DIR}/http/www
MOUNTS_CFG=${SCRIPT_DIR}/mounts.txt
GATEWAYS_CFG=${SCRIPT_DIR}/../../essentials/cfg/tests/gateways.conf

PORT=${1:-3000}

if [ "$2" = "--gdb" ]; then
    GDB="yag tool gdb --args"
fi

if [ -z "${GITHUB_BUILD_DIR}" ]; then
    PGM=${SCRIPT_DIR}/yqlrun
else
    PGM=${GITHUB_BUILD_DIR}/yql/tools/yqlrun/yqlrun
fi

${GDB} ${PGM} ui \
    --mounts ${MOUNTS_CFG} \
    --udfs-dir ${UDFS_DIR} \
    --assets ${ASSETS_DIR} \
    --gateways-cfg ${GATEWAYS_CFG} \
    --remote --port $PORT \
    $PG_EXT_OPT
