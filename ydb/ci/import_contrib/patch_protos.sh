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

echo "Patching protos..."

find $FOLDER -type f -name *.proto | while read f;do
    $sed -i -E 's/(^\s*import\s*)(\"ydb\/)/\1\"'$LOC_SED'\//g;
        s/github.com\/ydb-platform\/ydb\/ydb/a.yandex-team.ru\/contrib\/ydb/g;
        s/(option \s*go_package\s*=\s*\"a.yandex-team.ru\/)(ydb\/)/\1'$LOC_SED'\//g' $f
done

echo 'import sys

from contrib.ydb.public.api.grpc import *  # noqa
sys.modules["ydb._grpc.common"] = sys.modules["contrib.ydb.public.api.grpc"]
from contrib.ydb.public.api import protos  # noqa
sys.modules["ydb._grpc.common.protos"] = sys.modules["contrib.ydb.public.api.protos"]' > $FOLDER/public/sdk/python3/ydb/_grpc/common/__init__.py || true

echo "Patching protos completed."

