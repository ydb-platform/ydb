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
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/mkql
)


YQL_LAST_ABI_VERSION()


SRCS(
    spec.cpp
    spec.h
)

