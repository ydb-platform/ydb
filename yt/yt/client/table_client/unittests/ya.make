GTEST(unittester-client-table-client)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    columnar_statistics_ut.cpp
    columnar_ut.cpp
    serialization_ut.cpp
    unversioned_row_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client
    yt/yt/library/formats
    yt/yt/client/table_client/unittests/helpers
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
