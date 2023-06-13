LIBRARY()

SRCS(
    types_metadata.cpp
    functions_metadata.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/scheme_types
    ydb/library/yql/minikql/invoke_builtins
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
