YQL_LIBRARY()

SRCS(row_program.cpp)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/protos
    ydb/core/scheme
    ydb/library/mkql_proto
    ydb/services/udf_store/wasm
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
    yql/essentials/minikql/runtime_settings
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/comp_nodes
)

END()
