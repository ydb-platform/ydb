LIBRARY()

SRCS(
    kikimr_program_builder.cpp
    mkql_engine_flat.cpp
    mkql_engine_flat_extfunc.cpp
    mkql_engine_flat_host.cpp
    mkql_keys.cpp
    mkql_proto.cpp
    mkql_proto.h
)

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/deprecated/enum_codegen
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/base
    ydb/core/scheme
    ydb/core/tablet
    ydb/library/mkql_proto
    ydb/library/mkql_proto/protos
#    ydb/library/mkql_proto/ut/helpers
    ydb/public/api/protos
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/decimal
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    minikql
)

RECURSE_FOR_TESTS(
    ut
)
