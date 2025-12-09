FOLDER=$1

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

[ -z "${FOLDER}" ] && FOLDER=~/gh/ydb/ydb
[ -z "${LOC_NEW}" ] && LOC_NEW='contrib/ydb'
LOC_SED=$(echo $LOC_NEW | sed -e 's/\//\\\//g')

echo "Patching configs..."

find $FOLDER -type f -regex ".*cfg$" | while read f;do
    $sed -i -E 's/(;|"|\x27|\s|\^)(ydb\/)(public|library|tests)/\1'$LOC_SED'\/\3/g' $f
done

find $FOLDER -type f -name ".gitignore" | while read f; do
    cp $f $(dirname $f)/.arcignore
done
