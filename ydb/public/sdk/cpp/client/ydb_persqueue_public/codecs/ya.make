LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    codecs.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic/codecs
)

END()
