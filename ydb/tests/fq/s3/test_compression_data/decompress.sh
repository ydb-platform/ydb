#!/bin/bash

# example: ./decompress.sh big.json

echo gzip
time gzip -k -d -c -- "$1.gz" > /dev/null # gzip

echo lz4
time lz4 -k -d -c -- "$1.lz4" > /dev/null # lz4

echo brotli
time brotli -k -d -c -- "$1.br" > /dev/null # brotli

echo bzip2
time bzip2 -k -d -c -- "$1.bz2" > /dev/null # bzip1

echo zstd
time zstd -k -d -c -- "$1.zst" > /dev/null # zstd 

echo xz
time xz -k -d -c -- "$1.xz" > /dev/null # xz
