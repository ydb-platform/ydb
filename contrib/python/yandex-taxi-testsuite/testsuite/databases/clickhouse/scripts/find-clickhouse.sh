#!/bin/sh

find_clickhouse() {
    CLICKHOUSE_BINPATH=$(which clickhouse)
    if [ "x$CLICKHOUSE_BINPATH" = "x" ]; then
        return 1
    fi
    return 0
}

find_clickhouse ||
    die "No clickhouse server binary found.

For debian please run these commands:
sudo apt update && apt install clickhouse-common-static
(https://clickhouse.com/docs/en/getting-started/install/#packages),

For macos follow instructions at https://clickhouse.com/docs/en/getting-started/install/#from-binaries-non-linux

If you already have clickhouse installed, please symlink it to /usr/bin/clickhouse
"
