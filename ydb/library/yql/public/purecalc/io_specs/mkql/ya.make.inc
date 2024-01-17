SRCDIR(
    ydb/library/yql/public/purecalc/io_specs/mkql
)

ADDINCL(
    ydb/library/yql/public/purecalc/io_specs/mkql
)

PEERDIR(
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/lib/mkql_helpers
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/mkql
)


YQL_LAST_ABI_VERSION()


SRCS(
    spec.cpp
    spec.h
)

