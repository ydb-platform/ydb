PROGRAM()

SRCS(
    dq_compatibility_test.cpp
)

CFLAGS(
    -DUDF_ABI_VERSION_MAJOR=2
    -DUDF_ABI_VERSION_MINOR=36
    -DUDF_ABI_VERSION_PATCH=0
)

CXXFLAGS(
    -Wno-error
    -w
)

PEERDIR(
    ydb/library/yql/dq/proto
    library/cpp/getopt
    util
)

END() 