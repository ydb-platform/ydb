UNITTEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

IF (BUILD_TYPE == "RELEASE" OR BUILD_TYPE == "RELWITHDEBINFO")
    SRCS(
        huge_cluster.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    ydb/library/actors/testlib
)

REQUIREMENTS(
    cpu:1
    ram:32
)

END()
