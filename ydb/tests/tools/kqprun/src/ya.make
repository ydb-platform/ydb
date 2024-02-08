LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    ydb/core/testlib

    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
