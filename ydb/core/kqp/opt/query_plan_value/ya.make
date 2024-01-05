LIBRARY()

SRCS(
    kqp_query_plan_value.cpp
)

PEERDIR(
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/public/lib/value
)

END()
