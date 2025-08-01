LIBRARY()

SRCS(
    util.cpp
)

PEERDIR(
    contrib/libs/re2
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/common/proto
    ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
