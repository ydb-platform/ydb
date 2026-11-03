LIBRARY()

SRCS(
    float8_distance.cpp
    float8_distance_sse.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    SRC_C_AVX2(float8_distance_avx2.cpp -mf16c -mno-fma)
ELSE()
    SRC(float8_distance_avx2.cpp)
ENDIF()

PEERDIR(
    library/cpp/dot_product
    library/cpp/sse
)

END()

RECURSE_FOR_TESTS(
    ut
)
