LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    jwt.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/json
)

END()
