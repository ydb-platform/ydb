RECURSE(
    inference
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/curl

    ydb/core/fq/libs/config/protos
    ydb/library/actors/core
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/credentials
)

SRC(
    s3_fetcher.cpp
)

END()
