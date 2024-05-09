# Use this script to build YDB docs with Open Source tools and start HTTP server
# You may specify output directory as a parameter. If omitted, docs will be generated to a TEMP subdirectory

echo Checking YFM installed...
yfm --version
if [[ $? -ge 1 ]]; then
  echo
  echo "You need to have YFM builder (https://diplodoc.com/docs/en/tools/docs/) installed to run this script, exiting"
  exit
fi

echo Checking Python3 installed...
python3 --version

if [[ $? -ge 1 ]]; then
  echo
  echo "You need to have Python3 (https://www.python.org/) installed to run this script, exiting"
  exit
fi

DIR=${1:-"$TMPDIR"docs}

echo Starting YFM builder
echo Output directory: $DIR

yfm -i . -o $DIR --allowHTML

if [[ $? -ge 1 ]]; then
  echo
  echo ================================
  echo YFM build completed with ERRORS!
  echo ================================
fi

echo
echo Starting HTTP server, open the links in your browser:
echo
echo "- http://localhost:8888/en (English)"
echo "- http://localhost:8888/ru (Russian)"
echo
echo Press Ctrl+C in this window to stop the HTTP server.
echo

python3 -m http.server 8888 -d $DIR


