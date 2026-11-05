#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 || "${1:-}" == --help ]]; then
    cat <<'USAGE'
Usage: ydb/tools/build_local/run.sh <ya make arguments>
       ydb/tools/build_local/run.sh --check
       ydb/tools/build_local/run.sh --shell

Environment:
  YDB_BUILD_CPUS        Docker CPU quota (default: 4)
  YDB_BUILD_MEMORY      Docker memory limit, without swap (default: 24g)
  YA_MEMORY_LIMIT       ya build scheduler memory budget (default: 20G)
  YDB_BUILD_PLATFORM    Container platform (default: linux/amd64)
  YDB_BUILD_TYPE        ya build type (default: relwithdebinfo)
  YDB_BUILD_OUTPUT      Host output directory (default: .ydb-build/<platform>)
  YDB_BUILD_CACHE       Docker volume name for the platform-specific ya cache
  YDB_BUILD_UID/GID     Build user IDs (Linux: host IDs; macOS: 0 for VM mounts)
  DOCKER_CONTEXT       Optional Docker context, e.g. colima-ydb-build
USAGE
    exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)
platform=${YDB_BUILD_PLATFORM:-linux/amd64}
case "$platform" in
    linux/amd64|linux/arm64) ;;
    *) printf 'Unsupported YDB_BUILD_PLATFORM: %s\n' "$platform" >&2; exit 2 ;;
esac
platform_name=${platform//\//-}
default_uid=$(id -u)
default_gid=$(id -g)
if [[ $(uname -s) == Darwin ]]; then
    default_uid=0
    default_gid=0
fi
builder_uid=${YDB_BUILD_UID:-$default_uid}
builder_gid=${YDB_BUILD_GID:-$default_gid}
image_name="ydb-ya-build:ubuntu22.04-${platform_name}-${builder_uid}-${builder_gid}"
cache_name=${YDB_BUILD_CACHE:-ydb-ya-cache-${platform_name}-${builder_uid}-${builder_gid}}
output_dir=${YDB_BUILD_OUTPUT:-$repo_root/.ydb-build/$platform_name}

docker info >/dev/null
mkdir -p -- "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd)
docker build --platform "$platform" \
    --build-arg "BUILDER_UID=$builder_uid" --build-arg "BUILDER_GID=$builder_gid" \
    --tag "$image_name" "$script_dir"

run_options=(
    --rm --init --platform "$platform"
    --cpus "${YDB_BUILD_CPUS:-4}"
    --memory "${YDB_BUILD_MEMORY:-24g}" --memory-swap "${YDB_BUILD_MEMORY:-24g}"
    --mount "type=bind,source=$repo_root,target=/src,readonly"
    --mount "type=bind,source=$output_dir,target=/out"
    --mount "type=volume,source=$cache_name,target=/home/builder/.ya"
    --env "YA_MEMORY_LIMIT=${YA_MEMORY_LIMIT:-20G}"
    --env "YDB_BUILD_TYPE=${YDB_BUILD_TYPE:-relwithdebinfo}"
)
if [[ -t 0 && -t 1 ]]; then
    run_options+=(-it)
fi

exec docker run "${run_options[@]}" "$image_name" "$@"
