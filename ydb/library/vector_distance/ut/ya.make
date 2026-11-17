UNITTEST_FOR(ydb/library/vector_distance)

SRCS(
    int4_ut.cpp
    float8_ut.cpp
    float8_load_ut.cpp
    float8_torch_ut.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    CFLAGS(-DTEST_INT4_AVX2 -DTEST_FLOAT8_AVX2)
    SRC_C_AVX2(int4_load_avx2_ut.cpp -mno-fma)
    SRC_C_AVX2(float8_load_avx2_ut.cpp -mf16c -mno-fma)
ENDIF()

PEERDIR(
    library/cpp/l1_distance
    library/cpp/l2_distance
)

END()
