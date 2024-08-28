#!/usr/bin/env bash
set -eu

VERSION="16.3"

errexit() {
    echo $1
    exit 1
}



DIST="postgresql-$VERSION"

mkdir -p build
cd build

echo cleanup
rm -rf postgresql $DIST src_files
rm -rf $DIST.tar.gz
echo downloading sources
wget -O $DIST.tar.gz "https://ftp.postgresql.org/pub/source/v$VERSION/$DIST.tar.gz" || errexit "Download failed"
tar zxf $DIST.tar.gz
mv $DIST postgresql
cd postgresql
echo patching postgresql sources
patch -p0 < ../../source16.patch || errexit "Source patching failed"

COMPILER=$(ya tool cc --print-path)
TOOL_DIR=$(dirname $COMPILER)
export PATH="$TOOL_DIR:$PATH"
export CC=clang
export AR=llvm-ar
export CFLAGS="-ffunction-sections -fdata-sections -DWAIT_USE_SELF_PIPE"

echo configuring
./configure \
    --with-openssl --with-icu --with-libxml --enable-debug --without-systemd \
    --without-gssapi --with-lz4 --with-ossp-uuid \
    || errexit "Configure failed"
echo building
make -C src/backend all || errexit "Compilation failed"
cd ..
echo collecting *.c file list
for i in $(find postgresql -name "*.o" | sed -e 's/o$/c/'); do if [ -f $i ]; then realpath --relative-to=.  $i; fi; done > src_files
echo collecting *.h file list
find postgresql -type f -name "*.h" | sort >> src_files
find postgresql -type f -name "*.funcs.c" | sort >> src_files
find postgresql -type f -name "*.switch.c" | sort >> src_files
find postgresql -type f -name "regc_*.c" | sort >> src_files
find postgresql -type f -name "rege_dfa.c" | sort >> src_files
sort src_files > src_files.s
mv src_files.s src_files

echo copy data files...
rm -f ../postgresql/src/include/catalog/*.dat ../postgresql/src/backend/catalog/*.sql
cp postgresql/src/include/catalog/*.dat ../postgresql/src/include/catalog/
cp postgresql/src/backend/catalog/*.sql ../postgresql/src/backend/catalog/

cd ..
echo copy and patch src files...
python3 copy_src.py build
