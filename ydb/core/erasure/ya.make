LIBRARY()

SRCS(
    erasure.cpp
    erasure.h
    erasure_isa.cpp
    erasure_block82.cpp
    erasure_perf_test.cpp
    erasure_split.cpp
    erasure_restore.cpp
)

PEERDIR(
    contrib/libs/isa-l/erasure_code
    ydb/library/actors/util
    library/cpp/containers/stack_vector
    library/cpp/digest/crc32c
    library/cpp/digest/old_crc
    ydb/core/debug
)

IF (MSVC)
    CFLAGS(
        /wd4503
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
    ut_perf
)
