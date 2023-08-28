LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

IF (ARCH_X86_64)
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kHyperscan
    )

    PEERDIR(
        ydb/library/rewrapper/hyperscan
    )

ELSE()
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kRe2
    )

ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/json
    ydb/library/rewrapper/re2
    ydb/library/rewrapper
    ydb/library/binary_json
    ydb/library/yql/minikql/dom
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf
    ydb/library/yql/ast
    ydb/library/yql/utils
    ydb/library/yql/core/issue/protos
    ydb/library/yql/parser/proto_ast
    ydb/library/yql/parser/proto_ast/gen/jsonpath
)

SRCS(
    ast_builder.cpp
    ast_nodes.cpp
    binary.cpp
    executor.cpp
    jsonpath.cpp
    parse_double.cpp
    type_check.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(ast_nodes.h)

END()

RECURSE(
    benchmark
)

RECURSE_FOR_TESTS(
    ut
)
