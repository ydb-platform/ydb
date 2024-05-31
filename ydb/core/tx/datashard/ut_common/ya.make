LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/s3/s3_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_common.cpp
    datashard_ut_common.h
)

END()
