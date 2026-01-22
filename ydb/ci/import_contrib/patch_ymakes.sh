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

echo "Patching ya.makes..."

find $FOLDER -name ya.make -o -name 'ya.make.*' -o -name '*.inc' -o -name '*.make' | while read f;do
    if [ "$f" == $FOLDER/public/sdk/python2/ya.make ];then
        $sed -i -E 's/(^\s*)(ydb\/public\/\b)/\1'$LOC_SED'\/public\//g' $f
    elif [ "$f" == $FOLDER/public/sdk/python3/ya.make ];then
        $sed -i -E 's/(^\s*)(ydb\/public\/\b)/\1'$LOC_SED'\/public\//g' $f
    else
        # XXX: Do not touch YQL_UDF_YDB_TEST substitution, to avoid
        # breaking YDB import for the old branches.
        $sed -i -E 's/(^\s*|^\s*PEERDIR\(\s*|^\s*DEPENDS\(\s*|^\s*ADDINCL \s*|^\s*ADDINCL\s*\(\s*GLOBAL \s*|\s*GLOBAL \s*|^\s*PREFIX \s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*PY_MAIN\(\s*)(ydb\.\b)/\1'$LOC_DOT'\./g;
            s/(^\s*(UNITTEST_FOR|GO_TEST_FOR)\(\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*PY_PROTOS_FOR\(\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*SRCDIR\(\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(\$\{ARCADIA_ROOT\}\/)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/((\s*DATA\(|^)\s*arcadia\/)(ydb\/)/\1'$LOC_SED'\//g;
            s/(^\s*SET\s*\(\s*PROTOBUF_HEADER_PATH\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/^\s*(YQL_UDF|YQL_UDF_YDB)\(/YQL_UDF_CONTRIB\(/g;
            s/^\s*YQL_UDF_YDB_TEST\(/YQL_UDF_TEST\(/g;
            s/(^\s*GENERATE_ENUM_SERIALIZATION\(\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*USE_RECIPE\(\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*CPP_PROTO_PLUGIN0\(\s*validation\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/(^\s*CPP_PROTO_PLUGIN0\(\s*config_proto_plugin\s*)(ydb\/\b)/\1'$LOC_SED'\//g;
            s/\/\.\.\/contrib\/libs\//\/\.\.\/\.\.\/contrib\/libs\//g;
            s/(\$\{ARCADIA_BUILD_ROOT\}\/)(ydb\/)/\1'$LOC_SED'\//g;
            s/(\$ARCADIA_BUILD_ROOT\/)(ydb\/)/\1'$LOC_SED'\//g;
            s/(\$\{BINDIR\}\/)(ydb\/)/\1'$LOC_SED'\//g;
            s/(\$\{MODDIR\}\/)(ydb\/)/\1'$LOC_SED'\//g;
            s/( ydb\/)(library\/yql)/ '$LOC_SED'\/\2/g;
            s/( ydb\/)(tests\/tools)/ '$LOC_SED'\/\2/g;
            s/( ydb\/)(tests\/functional)/ '$LOC_SED'\/\2/g;
            s/=ydb\/tests/='$LOC_SED'\/tests/g;
            s/\"ydb\/tests/\"'$LOC_SED'\/tests/g;
            s/\"ydb\/apps\/ydbd\/ydbd/\"'$LOC_SED'\/apps\/ydbd\/ydbd/g;
            s/\"ydb\/core\/ymq\/client\/bin\/sqs/\"'$LOC_SED'\/core\/ymq\/client\/bin\/sqs/g;
            s/\"ydb\/apps\/pgwire\/pgwire/\"'$LOC_SED'\/apps\/pgwire\/pgwire/g;
            s/\"ydb\/apps\/ydb\/ydb/\"'$LOC_SED'\/apps\/ydb\/ydb/g;
            s/\"ydb\/library\/yql\/tools\/yqlrun\/yqlrun/\"'$LOC_SED'\/library\/yql\/tools\/yqlrun\/yqlrun/g;
            s/\"ydb\/library\/yql\/tools\/mrjob/\"'$LOC_SED'\/library\/yql\/tools\/mrjob/g' $f
    fi
done

$sed -i -E 's/(^\s*)ydb$/\1'$LOC_SED'/g' $FOLDER/apps/ydbd/ya.make || true
$sed -i -E 's/(^#include <)(ydb)(\/library\/yql)/\1'$LOC_SED'\3/g' $FOLDER/library/yql/minikql/computation/mkql_computation_node_codegen.h.txt || true
$sed -i -E 's/(^#include <)(ydb)(\/library\/yql)/\1'$LOC_SED'\3/g' $FOLDER/library/yql/minikql/invoke_builtins/mkql_builtins_codegen.h.txt || true

echo "Patching ya.makes completed."
