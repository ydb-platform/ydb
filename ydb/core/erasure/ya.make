LIBRARY()

SRCS(
    erasure.cpp
    erasure.h
    erasure_rope.cpp
    erasure_rope.h
    erasure_perf_test.cpp
)

PEERDIR(
    library/cpp/actors/util
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
    ut
    ut_rope
    ut_perf
)
