#!/bin/bash

# Use this script to build YDB docs with open-source tools
# You may specify the output directory as a parameter. If omitted, the docs will be generated to a TEMP subdirectory

set -e

check_dependency() {
  if ! command -v $1 &> /dev/null; then
    echo
    echo "You need to have $2 installed to run this script, exiting"
    echo "Installation instructions: $3"
    exit 1
  fi
}

DIR=${1:-"$TMPDIR"docs}

check_dependency "yfm" "YFM builder" "https://diplodoc.com/docs/en/tools/docs/"

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

