#!/bin/bash

# Use this script to build YDB docs and start an HTTP server
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

# Check for python3 dependency needed for the server
check_dependency "python3" "Python3" "https://www.python.org/downloads/"

# Build the documentation
echo "Building documentation..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if ! "$SCRIPT_DIR/build.sh" "$DIR"; then
  echo
  echo "Build failed! Cannot start server."
  exit 1
fi

# Start the HTTP server if build was successful
start_server "$DIR"