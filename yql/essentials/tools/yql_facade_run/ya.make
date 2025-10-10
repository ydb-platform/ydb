LIBRARY()

SRCS(
    yql_facade_run.cpp
)

PEERDIR(
    yql/essentials/providers/pg/provider
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/udf_resolve
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/defs
    yql/essentials/core/url_lister/interface
    yql/essentials/core/services/mounts
    yql/essentials/core/services
    yql/essentials/core/credentials
    yql/essentials/core/pg_ext
    yql/essentials/core/facade
    yql/essentials/core/url_lister
    yql/essentials/core/url_preprocessing
    yql/essentials/core/peephole_opt
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/qplayer/storage/file
    yql/essentials/core
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql
    yql/essentials/ast
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/parser/pg_catalog
    yql/essentials/public/udf
    yql/essentials/public/result_format
    yql/essentials/utils/failure_injector
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/protos
    yql/essentials/sql/settings
    yql/essentials/sql/v1/complete/check
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/check
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1
    yql/essentials/sql
    yql/essentials/public/langver
    yql/essentials/core/langver

    library/cpp/resource
    library/cpp/getopt
    library/cpp/yson/node
    library/cpp/yson
    library/cpp/logger

    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

END()
