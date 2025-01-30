#!/bin/sh

set -xue

rm -rf tmp
mkdir tmp
cp protoc* tmp/
cd tmp
tar czf ../protoc.tgz protoc*
