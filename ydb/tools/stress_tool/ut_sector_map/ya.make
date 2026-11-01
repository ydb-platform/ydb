UNITTEST_FOR(ydb/tools/stress_tool/lib)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    ../ut/device_test_tool_cli_ut.cpp
    device_test_tool_sector_map_ut.cpp
)

PEERDIR(
    ydb/apps/version
    ydb/core/blobstorage/pdisk
    ydb/library/pdisk_io
)

END()
