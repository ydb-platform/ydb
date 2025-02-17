LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    common.h
    common.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic/impl
)

END()
