#!/usr/bin/env bash
#
# Runs a command inside an ephemeral Porto container

set -euo pipefail

# Container limits
readonly CPU_LIMIT=1c
readonly MEMORY_LIMIT=1G

if [ -z "${PORTO_PROXY_LAYERS:-}" ]; then
    echo "porto_proxy: PORTO_PROXY_LAYERS is not set" >&2
    exit 1
fi

if [ "$#" -lt 1 ]; then
    echo "porto_proxy: missing binary path inside container" >&2
    exit 1
fi

LAYER_ARGS=()
for layer in $PORTO_PROXY_LAYERS; do
    LAYER_ARGS+=(-L "$layer")
done

CONTAINER_NAME="porto_proxy_$$_$RANDOM"
CONTAINER_USER="${PORTO_PROXY_USER:-nobody}"
CONTAINER_GROUP="${PORTO_PROXY_GROUP:-$(id -g -- "$CONTAINER_USER")}"

PROPS=(virt_mode=app weak=true enable_porto=false net=inherited)
PROPS+=("user=$CONTAINER_USER" "group=$CONTAINER_GROUP")
PROPS+=(capabilities= capabilities_ambient=)
PROPS+=("controllers[cpu]=true" "controllers[memory]=true" "controllers[blkio]=true")
PROPS+=("controllers[devices]=true" "controllers[freezer]=true" "controllers[pids]=true")
PROPS+=("cpu_limit=$CPU_LIMIT" "memory_limit=$MEMORY_LIMIT")
if [ -n "${PORTO_PROXY_MOUNTS:-}" ]; then
    PROPS+=("bind=$PORTO_PROXY_MOUNTS")
fi
if [ -n "${LD_LIBRARY_PATH:-}" ]; then
    PROPS+=("env[LD_LIBRARY_PATH]=$LD_LIBRARY_PATH")
fi

COMMAND=""
for arg in "$@"; do
    arg="${arg//\'/\'\\\'\'}"
    COMMAND="$COMMAND '$arg'"
done
PROPS+=("command=$COMMAND")

# Prevent portoctl info messages from polluting command output
# by redirecting them to stderr
exec portoctl exec -T "${LAYER_ARGS[@]}" "$CONTAINER_NAME" \
    "${PROPS[@]}" "stdout_path=/dev/fd/3" 3>&1 1>&2
