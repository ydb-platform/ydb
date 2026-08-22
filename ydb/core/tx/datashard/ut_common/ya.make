LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/core/kqp/ut/common
    ydb/library/ut
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_common.cpp
    datashard_ut_common.h
    datashard_ut_common_tx.cpp
    datashard_ut_common_tx.h
)

END()
