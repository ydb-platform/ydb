#!/bin/sh -e
make S3_ACCESS_KEY=$1 S3_SECRET_KEY=$2 -rkj -f test.mk all.test.dst && echo OK || echo Error
