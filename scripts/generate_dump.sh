#!/usr/bin/env bash

set -e

TMP_FOLDER='/tmp/ydb-build-dump'

rm -rf $TMP_FOLDER
mkdir $TMP_FOLDER

PLATFORM=$1
TARGET_PLATFORM=$2

PARAMS_FILE=$TMP_FOLDER/params.json
UNIX_TEMPLATE=$TMP_FOLDER/unix_template.txt
WIN_TEMPLATE=$TMP_FOLDER/win_template.txt

cat <<'EOF'>$UNIX_TEMPLATE
{
  "env": {
    "CPATH": [""],
    "LIBRARY_PATH": [""],
    "SDKROOT": [""]
  },
  "params": {
    "c_compiler": "$$($platform)/bin/clang",
    "cxx_compiler": "$$($platform)/bin/clang++",
    "version": "14.0",
    "llvm-symbolizer": "$$($platform)/bin/llvm-symbolizer",
    "match_root": "$platform",
    "objcopy": "$$($platform)/bin/llvm-objcopy",
    "strip": "$$($platform)/bin/llvm-strip",
    "type": "clang",
    "werror_mode": "all"
  },
  "platform": {
    "host": {
      "arch": "$arch",
      "os": "$os",
      "toolchain": "default",
      "visible_name": "clang14"
    },
    "target": {
      "arch": "$arch",
      "os": "$os",
      "toolchain": "default",
      "visible_name": "clang14"
    }
  }
}
EOF

cat <<'EOF'>$WIN_TEMPLATE
{
  "params": {
    "c_compiler": "$$($platform)/bin/Hostx64/x64/cl.exe",
    "cxx_compiler": "$$($platform)/bin/Hostx64/x64/cl.exe",
    "cxx_std": "c++latest",
    "for_ide": "msvs2019",
    "match_root": "$platform",
    "type": "msvc",
    "version": "2019",
    "werror_mode": "compiler_specific"
  },
  "platform": {
    "host": {
      "arch": "$arch",
      "os": "$os",
      "toolchain": "default",
      "visible_name": "msvc2019"
    },
    "target": {
      "arch": "$arch",
      "os": "$os",
      "toolchain": "default",
      "visible_name": "msvc2019"
    }
  }
}
EOF

if [[ $PLATFORM = win* ]]
then
    TEMPLATE=$WIN_TEMPLATE
else
    TEMPLATE=$UNIX_TEMPLATE
fi

DUMP_EXPORT_PATH="$TMP_FOLDER/ymake.$PLATFORM.conf"

# generate params for ymake_conf.py
if base64 --help | grep -q -e '^\s*-w,';then
  base64cmd='base64 -w0'
else
  base64cmd='base64'
fi
python3 -c "import sys, string as s; v=sys.argv; p = v[1].replace('-', '_'); o, a = v[2].split('-'); print(s.Template(open('$TEMPLATE').read()).substitute(platform=p.upper(), arch=a, os=o.upper()))" $TARGET_PLATFORM $PLATFORM >$DUMP_EXPORT_PATH.params
PARAMS=`cat $DUMP_EXPORT_PATH.params | $base64cmd`

if [[ $PLATFORM = darwin* ]] ; then
   export SDKROOT="SDKROOT"
   DOSSDK="-D OS_SDK=local"
else
   DOSSDK=""
fi

ARCADIA=`realpath .`
python3 $ARCADIA/build/ymake_conf.py $ARCADIA release no --toolchain-params $PARAMS \
    -D NO_SVN_DEPENDS=yes -D REPORT_CONFIGURE_PROGRESS=yes -D EXPORT_CMAKE=yes -D TRAVERSE_RECURSE=yes -D TRAVERSE_RECURSE_FOR_TESTS=yes \
    -D BUILD_LANGUAGES=CPP -D EXPORTED_BUILD_SYSTEM_SOURCE_ROOT='${CMAKE_SOURCE_DIR}' -D EXPORTED_BUILD_SYSTEM_BUILD_ROOT='${CMAKE_BINARY_DIR}' \
    -D OPENSOURCE=yes -D OPENSOURCE_PROJECT=ydb -D HAVE_CUDA=no -D CUDA_VERSION=0.0 -D USE_PREBUILT_TOOLS=no $DOSSDK >$DUMP_EXPORT_PATH
# append new line
echo >>$DUMP_EXPORT_PATH

cat $DUMP_EXPORT_PATH
# cp $DUMP_EXPORT_PATH .

# rm -rf $TMP_FOLDER
