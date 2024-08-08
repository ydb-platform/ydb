UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/library/actors/interconnect/mock
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/tx/scheme_board
    ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

SRCS(
    backpressure_ut.cpp
    defs.h
    loader.h
    skeleton_front_mock.h
)

END()
