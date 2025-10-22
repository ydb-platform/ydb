GTEST()

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/external_sources/object_storage/inference
    ydb/core/external_sources/object_storage
    ydb/core/tx/scheme_board
    ydb/library/yql/providers/common/http_gateway/mock
    ydb/core/util/actorsys_test
)

SRCS(
    arrow_inference_ut.cpp
)

END()
