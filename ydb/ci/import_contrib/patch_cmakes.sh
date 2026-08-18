FOLDER=$1

[ -z "${FOLDER}" ] && FOLDER=~/gh/ydb/ydb
[ -z "${LOC_NEW}" ] && LOC_NEW='contrib/ydb'
LOC_SED=$(echo $LOC_NEW | sed -e 's/\//\\\//g')
LOC_DOT=$(echo $LOC_NEW | sed -e 's/\//\\\./g')

# sed --version > /dev/null 2> /dev/null
if ! sed --version > /dev/null 2> /dev/null ;then
  gsed --version > /dev/null 2> /dev/null
  if ! gsed --version > /dev/null 2> /dev/null ;then
    echo You seem to have an old version of sed installed on your machine. If on MacOS, run \'brew install gnu-sed\' to run this script.
    exit
  fi
  sed=gsed
else
  sed=sed
fi

echo "Patching epilogue.cmake..."

find $FOLDER -name epilogue.cmake | while read f;do
  $sed -i -E \
      's/\$\{CMAKE_SOURCE_DIR\}\/ydb/\$\{CMAKE_SOURCE_DIR\}\/'$LOC_SED'/g;
       s/\$\{CMAKE_BINARY_DIR\}\/ydb/\$\{CMAKE_BINARY_DIR\}\/'$LOC_SED'/g' $f
done

echo "Patching epilogue.cmake completed."
