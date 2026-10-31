UNITTEST_FOR(ydb/library/vector_distance)

SRCS(
    int4_ut.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    CFLAGS(-DTEST_INT4_AVX2)
    SRC_C_AVX2(int4_load_avx2_ut.cpp -mno-fma)
ENDIF()

PEERDIR(
    library/cpp/l1_distance
    library/cpp/l2_distance
)

END()
