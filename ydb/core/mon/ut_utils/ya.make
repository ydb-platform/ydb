LIBRARY()

SRCS(
    ut_utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

END()
