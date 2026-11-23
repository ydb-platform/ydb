UNITTEST_FOR(yql/essentials/core/project_settings)

SRCS(
    yql_project_settings_qstorage_ut.cpp
    yql_project_settings_ut.cpp
)

PEERDIR(
    yql/essentials/core/project_settings/convert
    yql/essentials/core/project_settings/proto
    library/cpp/yson/node
    yql/essentials/core
    yql/essentials/core/facade
    yql/essentials/core/qplayer/storage/memory
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/providers/config
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/translation
)

YQL_LAST_ABI_VERSION()

END()
