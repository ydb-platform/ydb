#!/bin/sh -e

#
# Usage: ./ui.sh PORT [--gdb]
#
# OPTIONS:
#     --gdb   run under gdb
#

SCRIPT_DIR="$(dirname $(readlink -f "$0"))"
UDFS_DIR="${SCRIPT_DIR}/../../udfs;${SCRIPT_DIR}/../../essentials/udfs;${SCRIPT_DIR}/../../../contrib/ydb/library/yql/udfs"
ASSETS_DIR=${SCRIPT_DIR}/http/www
MOUNTS_CFG=${SCRIPT_DIR}/mounts.txt
GATEWAYS_CFG=${SCRIPT_DIR}/../../essentials/cfg/tests/gateways.conf

if [ -f "${SCRIPT_DIR}/../../pg_ext/postgis/libs/postgis/libpostgis.so" ]; then
    PG_EXT_OPT="--pg-ext pg_ext.txt"
else
    PG_EXT_OPT=
fi


PORT=${1:-3000}

if [ "$2" = "--gdb" ]; then
    GDB="ya tool gdb --args"
fi

if [ -z "${ARC_BUILD_DIR}" ]; then
    PGM=${SCRIPT_DIR}/yqlrun
else
    PGM=${ARC_BUILD_DIR}/yql/tools/yqlrun/yqlrun
fi

${GDB} ${PGM} ui \
    --mounts ${MOUNTS_CFG} \
    --udfs-dir ${UDFS_DIR} \
    --assets ${ASSETS_DIR} \
    --gateways-cfg ${GATEWAYS_CFG} \
    --remote --port $PORT \
    $PG_EXT_OPT
