#!/bin/bash

set -eu

apt-get update && apt-get install -y patch

go install github.com/jstemmer/go-junit-report/v2@v2.0.0

mkdir -p /original-sources
cd /original-sources

wget https://github.com/lib/pq/archive/refs/tags/v1.10.9.tar.gz -O libpq.tar.gz
tar --strip-components=1 -zxvf libpq.tar.gz
rm -f libpq.tar.gz

mkdir -p /project/sources/
cp -R /original-sources/. /project/sources/

cd /project/sources/
[ -e /patch.diff ] && patch -s -p0 < /patch.diff

# cache binary
echo "Build test binary"
go test -c -o ./test.binary
