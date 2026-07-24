LIBRARY()

SRCS(
    dot_product.cpp
    dot_product_sse.cpp
    dot_product_simple.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    SRC_C_AVX2(dot_product_avx2.cpp -mfma)
    SRC_C_AVX512(dot_product_vnni.cpp -mavx512vnni)
ELSE()
    SRC(dot_product_avx2.cpp)
    SRC(dot_product_vnni.cpp)
ENDIF()

PEERDIR(
    library/cpp/sse
    library/cpp/testing/common
)

END()

RECURSE(
    ut
)
