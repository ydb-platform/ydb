#!/bin/sh

set -xue

rm -rf protoc protoc.exe protoc.tgz protoc.pdb tmp protoc.dSYM
ya make -r "${@}"
rm -f protoc.dSYM
sh ./pack.sh
ya upload --ttl=inf protoc.tgz
rm -rf protoc protoc.exe protoc.tgz protoc.pdb tmp protoc.dSYM
