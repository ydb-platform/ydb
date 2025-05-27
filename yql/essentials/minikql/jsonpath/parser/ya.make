LIBRARY()

IF (ARCH_X86_64)
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kHyperscan
    )
ELSE()
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kRe2
    )

ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/json
    yql/essentials/minikql/jsonpath/rewrapper
    yql/essentials/minikql/jsonpath/rewrapper/re2
    yql/essentials/public/issue
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/core/issue/protos
    yql/essentials/parser/proto_ast/antlr3
    yql/essentials/parser/proto_ast/gen/jsonpath
)

SRCS(
    ast_builder.cpp
    ast_nodes.cpp
    binary.cpp
    parser.cpp
    parse_double.cpp
    type_check.cpp
)

GENERATE_ENUM_SERIALIZATION(ast_nodes.h)

END()

