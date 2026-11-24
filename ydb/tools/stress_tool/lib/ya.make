LIBRARY(ydb_device_test)


PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/monlib/dynamic_counters/percentile
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/load_test/blobstorage
    ydb/core/protos
    ydb/tools/stress_tool/proto
    ydb/library/actors/core
)

SRCS(
    ../device_test_tool.h
    ../device_test_tool_cli.cpp
    ../device_test_tool_pdisk_test.h
)

IF (OS_LINUX)
    SRCS(
        ../device_test_tool_aio_test.h
        ../device_test_tool_driveestimator.h
        ../device_test_tool_trim_test.cpp
        ../device_test_tool_trim_test.h
    )
ENDIF()

END()

IF (OS_LINUX OR OS_DARWIN)
    RECURSE_FOR_TESTS(
        ../ut_sector_map
    )
ENDIF()

IF (OS_LINUX)
    RECURSE_FOR_TESTS(
        ../ut
    )
ENDIF()
