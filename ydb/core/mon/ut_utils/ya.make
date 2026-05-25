LIBRARY()

SRCS(
    ut_utils.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/testlib/default
    ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

END()
