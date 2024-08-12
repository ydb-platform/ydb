GTEST()

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/core/external_sources/object_storage/inference
    ydb/core/external_sources/object_storage
    ydb/core/tx/scheme_board
    ydb/library/yql/providers/common/http_gateway/mock
)

SRCS(
    arrow_inference_ut.cpp
)

END()
