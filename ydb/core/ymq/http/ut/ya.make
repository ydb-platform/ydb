UNITTEST()

PEERDIR(
    ydb/core/ymq/http
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    xml_builder_ut.cpp
)

END()
