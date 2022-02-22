LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

SRCS(
    codecs.h
    codecs.cpp
)

PEERDIR(
    library/cpp/streams/lzop
    library/cpp/streams/zstd
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/library/yql/public/issue/protos
)

PROVIDES(pq_codecs)

END()
