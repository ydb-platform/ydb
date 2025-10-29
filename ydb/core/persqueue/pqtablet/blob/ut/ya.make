UNITTEST_FOR(ydb/core/persqueue/pqtablet/blob)

SRCS(
    blob_ut.cpp
    type_codecs_ut.cpp
)

PEERDIR (
    ydb/core/persqueue/common
)

END()
