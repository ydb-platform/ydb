LIBRARY()

SRCS(
    pq_ut_common.cpp
    pq_ut_common.h
)

PEERDIR(
    ydb/core/testlib
    ydb/core/persqueue
)

YQL_LAST_ABI_VERSION()

END()
