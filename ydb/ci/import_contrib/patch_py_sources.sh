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
LOC_DOT=$(echo $LOC_NEW | sed -e 's/\//\\\./g')
LOC_SED=$(echo $LOC_NEW | sed -e 's/\//\\\//g')

echo "Patching PY sources..."

find $FOLDER -type f -regex '.*\.py$' | while read f;do
   $sed -i -E 's/(^\s*(from|import) \s*)(ydb\.)(tests|public|core|apps|library|tools)(\.)/\1'$LOC_DOT'\.\4\5/g;
    s/((from |_path\()("<|"|\x27))(ydb\/)(library|public)/\1'$LOC_SED'\/\5/g;
    s/"ydb\.public\./"'$LOC_DOT'\.public\./g;
    s/((\x27|")ydb\/)(library\/yql|tests|docs)/\2'$LOC_SED'\/\3/g;
    /pytest_plugins/s/ydb\./'$LOC_DOT'\./g' $f
done

echo "Patching PY sources completed."
