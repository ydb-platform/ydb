LIBRARY()

SRCS(
    util.cpp
)

PEERDIR(
    contrib/libs/re2
    ydb/library/yql/providers/solomon/proto
    yql/essentials/providers/common/proto
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
