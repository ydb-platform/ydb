UNITTEST_FOR(ydb/library/yql/utils/simd)

SRCS(
    simd_ut.cpp
    merge_ut.cpp
)

CFLAGS(-mavx2)

END()