UNITTEST()

PEERDIR(
    ydb/core/ymq/http
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    xml_builder_ut.cpp
)

END()
