#!/bin/sh

set -xue

find contrib/orc -type f | while read l; do
    sed -e 's|namespace orc|namespace orc_chdb|' \
        -e 's|orc::|orc_chdb::|g' \
        -e 's|orc_proto.pb.|orc_chdb_proto.pb.|g' \
        -i ${l}
done

find src/Processors/Formats/Impl/ORCBlockOutput* -type f | while read l; do
    sed -e 's|namespace orc|namespace orc_chdb|' \
        -e 's|orc::|orc_chdb::|g' \
        -i ${l}
done

find src/Processors/Formats/Impl/NativeORCBlockInput* -type f | while read l; do
    sed -e 's|namespace orc|namespace orc_chdb|' \
        -e 's|orc::|orc_chdb::|g' \
        -i ${l}
done

find contrib/orc/proto -type f | while read l; do
    sed -e 's|package orc.proto;|package orc_chdb.proto;|' \
        -e 's|org.apache.orc|org.apache.orc_chdb|' \
        -i ${l}
done
