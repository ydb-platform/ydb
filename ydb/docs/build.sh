#!/bin/bash

# Use this script to build YDB docs with open-source tools
# You may specify the output directory as a parameter. If omitted, the docs will be generated to a TEMP subdirectory

set -e

check_dependency() {
  if ! command -v "$1" &> /dev/null; then
    echo
    echo "You need to have $2 installed to run this script, exiting"
    echo "Installation instructions: $3"
    exit 1
  fi
}

check_dependency "yfm" "YFM builder" "https://diplodoc.com/docs/en/tools/docs/"
check_dependency "python3" "Python 3" "https://www.python.org/downloads/"

DIR=${1:-"$(python3 -c "import os; print(os.path.realpath('${TMPDIR:-/tmp}'))")docs"}

if ! python3 -c 'import grpc_tools, yaml' &> /dev/null; then
  echo
  echo "You need the feature flag generator dependencies installed to run this script, exiting"
  echo "Installation command: python3 -m pip install -r tools/feature_flags/requirements.txt"
  exit 1
fi

# Generate variables from the same checkout as the documentation.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRESETS="$SCRIPT_DIR/presets.yaml"
PRESETS_BACKUP="$(mktemp "${TMPDIR:-/tmp}/ydb-docs-presets.XXXXXX")"
cp "$PRESETS" "$PRESETS_BACKUP"
restore_presets() {
  cp "$PRESETS_BACKUP" "$PRESETS"
  rm -f "$PRESETS_BACKUP"
}
trap restore_presets EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

python3 "$SCRIPT_DIR/tools/feature_flags/generate.py"

echo "Starting YFM builder"
echo "Output directory: $DIR"

if ! yfm -s -i . -o $DIR --allowHTML --apply-presets; then
  echo
  echo '================================'
  echo 'YFM build completed with ERRORS!'
  echo '================================'
  echo 'It may be necessary to use the latest version of npm. Run the commands `nvm install v23.7.0` and `nvm use v23.7.0` to update it.'
  exit 1
fi

echo
echo "Build completed successfully!"
echo "Output directory: $DIR"
exit 0
