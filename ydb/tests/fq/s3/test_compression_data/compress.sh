#!/bin/bash

set -e

# example: ./compress.sh big.json

gzip -k -- "$1" # gzip
lz4 -k -- "$1" # lz4
brotli -- "$1" # brotli
bzip2 -k -- "$1" # bzip1
zstd -- "$1" # zstd 
xz -k -- "$1" # xz
