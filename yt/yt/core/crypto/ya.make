LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    crypto.cpp
    tls.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/openssl
    library/cpp/openssl/io
)

END()
