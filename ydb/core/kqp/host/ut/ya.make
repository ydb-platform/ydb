# Fast frontend unit tests exercise each phase directly, including rejection
# and fallback contracts. They need no actor runtime or provider metadata IO.
UNITTEST_FOR(ydb/core/kqp/host/new_parser)

SRCS(
    kqp_ast_ut.cpp
    kqp_new_parser_ut.cpp
    kqp_name_resolver_ut.cpp
    kqp_expr_lowering_ut.cpp
    kqp_type_annotation_ut.cpp
)

PEERDIR(
    yql/essentials/core
    yql/essentials/core/type_ann
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
