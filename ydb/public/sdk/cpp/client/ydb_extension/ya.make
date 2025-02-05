LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    extension.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
