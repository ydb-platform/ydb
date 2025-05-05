Y_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

ALLOCATOR(LF)

PEERDIR(
    library/cpp/threading/skip_list
    ydb/core/util
)

SRCS(
    main.cpp
)

END()
