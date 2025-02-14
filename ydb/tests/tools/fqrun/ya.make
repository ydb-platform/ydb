PROGRAM(fqprun)

SRCS(
    fqprun.cpp
)

PEERDIR(
    library/cpp/colorizer
    util
    ydb/tests/tools/fqrun/src
    ydb/tests/tools/kqprun/runlib
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
