LIBRARY()

SRCS(
    yql_mkql_file_input_state.cpp
    yql_mkql_file_list.cpp
    yql_mkql_input_stream.cpp
    yql_mkql_input.cpp
    yql_mkql_output.cpp
    yql_mkql_table_content.cpp
    yql_mkql_table.cpp
    yql_mkql_ungrouping_list.cpp
)

PEERDIR(
    library/cpp/streams/brotli
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/expr_nodes
)

# INCLUDE(../../../minikql/computation/header.ya.make.inc)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    dq
)

RECURSE_FOR_TESTS(
    ut
)
