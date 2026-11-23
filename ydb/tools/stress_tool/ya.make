IF (OS_LINUX OR OS_DARWIN)
    PROGRAM(ydb_stress_tool)

    PEERDIR(
        library/cpp/getopt
        ydb/apps/version
        ydb/core/base
        ydb/core/blobstorage/crypto
        ydb/core/blobstorage/lwtrace_probes
        ydb/core/blobstorage/pdisk
        ydb/core/load_test/blobstorage
        ydb/core/node_whiteboard
        ydb/core/tablet
        ydb/library/actors/interconnect
        ydb/library/pdisk_io
        ydb/tools/stress_tool/lib
        ydb/tools/stress_tool/proto
    )

    IF (OS_LINUX)
        PEERDIR(
            ydb/core/load_test/ddisk
            ydb/core/load_test/interconnect
            ydb/library/actors/dnsresolver
            yql/essentials/minikql/comp_nodes/llvm16
            yql/essentials/parser/pg_wrapper
            yql/essentials/sql/pg
            yt/yql/providers/yt/comp_nodes/dq/llvm16
            yt/yql/providers/yt/comp_nodes/llvm16
        )
    ENDIF()

    SRCS(
        device_test_tool.cpp
    )

    END()

    RECURSE_FOR_TESTS(
        ut_sector_map
    )

    IF (OS_LINUX)
        RECURSE_FOR_TESTS(
            ut
        )
    ENDIF()
ENDIF()
