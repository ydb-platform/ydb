#!/bin/bash

# Use this script to build YDB docs with open-source tools and start an HTTP server
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

start_server() {
  echo
  echo "Starting HTTP server, open the links in your browser:"
  echo
  echo "- http://localhost:8888/en (English)"
  echo "- http://localhost:8888/ru (Russian)"
  echo
  echo "Press Ctrl+C in this window to stop the HTTP server."
  echo
  python3 -m http.server 8888 -d $1
}

DIR=${1:-"$TMPDIR"docs}

check_dependency "yfm" "YFM builder" "https://diplodoc.com/docs/en/tools/docs/"
check_dependency "python3" "Python3" "https://www.python.org/downloads/"

echo "Starting YFM builder"
echo "Output directory: $DIR"

if ! yfm -i . -o $DIR --allowHTML; then
  echo
  echo "================================"
  echo "YFM build completed with ERRORS!"
  echo "================================"
  exit 1
fi

start_server $DIR

