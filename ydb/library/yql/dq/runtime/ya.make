LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/yverify_stream
    ydb/library/mkql_proto
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/udf
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/dq/common
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/type_ann
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/actors/util
)

SRCS(
    dq_arrow_helpers.cpp
    dq_async_input.cpp
    dq_async_output.cpp
    dq_columns_resolve.cpp
    dq_compute.cpp
    dq_input_channel.cpp
    dq_input_producer.cpp
    dq_output_channel.cpp
    dq_output_consumer.cpp
    dq_tasks_runner.cpp
    dq_transport.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_tasks_runner.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
