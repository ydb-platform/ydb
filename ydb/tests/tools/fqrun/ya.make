PROGRAM(fqrun)

SRCS(
    fqrun.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    util
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/tests/tools/fqrun/src
    ydb/tests/tools/kqprun/runlib
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

PEERDIR(
    yql/essentials/udfs/common/compress_base
)

YQL_LAST_ABI_VERSION()

END()
