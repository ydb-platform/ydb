LIBRARY()

PEERDIR(
    ydb/library/yql/public/purecalc/common
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

END()

RECURSE_FOR_TESTS(
    ut
)
