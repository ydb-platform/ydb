UNITTEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

IF (BUILD_TYPE == "RELEASE" OR BUILD_TYPE == "RELWITHDEBINFO")
    SRCS(
        huge_cluster.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    library/cpp/actors/interconnect/ut/lib
    library/cpp/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    library/cpp/actors/testlib
)

REQUIREMENTS(
    cpu:4
    ram:32
)

END()
