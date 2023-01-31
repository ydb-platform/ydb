#!/usr/bin/env bash
set -e

# Avoid dependency on locale
LC_ALL=C

# script relative current directory
CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# default supported arch
DEB_ARCH=${DEB_ARCH:-amd64}

HELP="${0} [--help]
  --deb - build deb package
  --help - show this help and exit
Required envs:
  RELEASE_DIR='${RELEASE_DIR:-"!EMPTY!"}' - where build the artifacts located
  YDB_VERSION_STRING='${YDB_VERSION_STRING:-"!EMPTY!"}' - the package version to use, must be specified
Optional envs:
  DEB_ARCH='${DEB_ARCH}' - target cpu architecture
"

if [ $# -eq 0 ]; then
  echo "$HELP"
  exit 1
fi

# other package types can be added
# must be specified package types:
# https://nfpm.goreleaser.com/configuration/

while [[ $1 == --* ]]; do
  case "$1" in
  --deb)
    YDB_PACKAGE_PACKAGER="deb"
    # platform specific depends syntax
    # var referenced in ydb*.template.yaml configs
    export YDB_PACKAGE_DEPENDS_LIBC="libc6 (>= 2.30.0)"
    export YDB_PACKAGE_DEPENDS_YDB_SERVER="ydb-server (>= $YDB_VERSION_STRING)"
    export YDB_PACKAGE_DEPENDS_JQ="jq (>= 1.6)"
    shift
    ;;
  --help)
    echo "$HELP"
    exit 1
    ;;
  *)
    echo "Unknown option $1"
    exit 2
    ;;
  esac
done

# must be specified as env var
YDB_VERSION_STRING=${YDB_VERSION_STRING:?"ydb version is missing, set with YDB_VERSION_STRING env"}

# must be specified as env var
RELEASE_DIR=${RELEASE_DIR:?"release dir is missing, set with RELEASE_DIR env"}

# relative to release dir
OUTPUT_DIR="${RELEASE_DIR}/packages"

if [ ! -d "${OUTPUT_DIR}" ]; then
  mkdir -p "${OUTPUT_DIR}"
fi

for config_folder in "$CURRENT_DIR"/ydb*; do
  for config_template in "$config_folder"/ydb*.template.yaml; do
    config="${config_template/.template/}"

    echo "Replace template variables for $config_template to $config"
    envsubst <"$config_template" >"$config"

    echo "Building deb package for $config"
    nfpm package --target "$OUTPUT_DIR" --config "$config" --packager "$YDB_PACKAGE_PACKAGER"

    echo "Cleanup generated config $config"
    rm -f "$config"
  done
done
