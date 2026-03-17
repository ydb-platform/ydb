#!/bin/sh

find_pg() {
    POSTGRESQL_BINPATH=
    for pgdir in $*; do
        if [ -x "$pgdir/pg_ctl" ]; then
            POSTGRESQL_BINPATH="$pgdir"
            return 0
        fi
    done
    return 1
}

find_pg $TESTSUITE_PGSQL_BINDIR \
        $(pg_config --bindir 2>/dev/null) ||
    die "No PostgreSQL installation found. Install it or set
TESTSUITE_PGSQL_BINDIR environment variable to the directory containing
pg_ctl binary."
