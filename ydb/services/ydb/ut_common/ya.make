LIBRARY()

SRCS(
    ydb_ut_common.cpp
    ydb_ut_common.h
    ydb_ut_test_includes.h
)

PEERDIR(
    library/cpp/testing/common
    contrib/libs/grpc
    ydb/core/base
    ydb/core/testlib
    ydb/public/api/grpc
    yql/essentials/core/issue
    ydb/public/sdk/cpp/src/library/issue
)

END()
