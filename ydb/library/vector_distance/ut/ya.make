UNITTEST_FOR(ydb/library/vector_distance)

SRCS(
    float8_ut.cpp
    float8_load_ut.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    CFLAGS(-DTEST_FLOAT8_AVX2)
    SRC_C_AVX2(float8_load_avx2_ut.cpp)
ENDIF()

END()
