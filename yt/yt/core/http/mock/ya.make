LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    http.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/yt/core/http
)

END()
