FOLDER=$1

[ -z "${FOLDER}" ] && FOLDER=~/gh/ydb/ydb
[ -z "${LOC_NEW}" ] && LOC_NEW='contrib/ydb'
LOC_SED=$(echo $LOC_NEW | sed -e 's/\//\\\//g')

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

echo "Patching sources..."

find $FOLDER -type f -regex ".*\(h\|h.in\|cpp\|cpp.in\|c\|ipp\|jnj\|rl6\|h.txt\)$" | while read f;do
    $sed -i -E 's/(^\s*#include\s*)(<|\")(ydb\/)/\1\2'$LOC_SED'\//g;
               s/\/\.\.\/contrib\/libs\//\/\.\.\/\.\.\/contrib\/libs\//g' $f
done

echo "Patching GO sources with formatting..."
find $FOLDER -type f -name *.go | while read f;do
    echo $f
    $sed -i -E 's/github.com\/ydb-platform\/ydb\/library\/go/a.yandex-team.ru\/library\/go/g;
        s/(github.com\/ydb-platform\/ydb\/ydb|a.yandex-team.ru\/ydb)/a.yandex-team.ru\/contrib\/ydb/g' $f
done | xargs -r $ARC_ROOT/ya tool yoimport -w

echo Special fixes...

$sed -i -E 's/(^\s*#include\s*\\)(<|\")(ydb\/)/\1\2'$LOC_SED'\//g' $FOLDER/library/yql/parser/proto_ast/org/antlr/codegen/templates/Cpp/Cpp.stg.in || true
$sed -i -E 's/(^\s*#include\s*\\)(<|\")(ydb\/)/\1\2'$LOC_SED'\//g' $FOLDER/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in || true
$sed -i -E 's/\/\.\.\/util\//\/\.\.\/\.\.\/util\//g' $FOLDER/core/ymq/http/parser.rl6 || true
find $FOLDER -name toc.yaml | xargs -I{} $sed -i -E 's|path: ydb/docs/|path: contrib/ydb/docs/|g' {} || true

echo "Patching sources completed."
