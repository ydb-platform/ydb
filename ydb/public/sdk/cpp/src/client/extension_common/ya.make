LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    extension.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/sdk/cpp/src/client/driver
)

END()
