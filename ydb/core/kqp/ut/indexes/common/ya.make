LIBRARY()

SRCS(
    kqp_indexes_ttl_ut_common.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
