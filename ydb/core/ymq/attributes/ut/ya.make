UNITTEST()

PEERDIR(
    ydb/core/ymq/attributes
    ydb/core/persqueue/public
    ydb/public/api/protos
)

SRCS(
    attributes_md5_ut.cpp
    attributes_ut.cpp
)

END()
