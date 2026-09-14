LIBRARY()

SRCS(
    knn-half-distance.cpp
    knn-half-distance-sse.cpp
)

IF (USE_SSE4 == "yes" AND OS_LINUX == "yes")
    SRC_C_AVX2(knn-half-distance-avx2.cpp -mf16c)
ELSE()
    SRC(knn-half-distance-avx2.cpp)
ENDIF()

PEERDIR(
    library/cpp/dot_product
    library/cpp/sse
)

END()
