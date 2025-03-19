LIBRARY()

SRCS(
    yql_facade.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/random_provider
    library/cpp/string_utils/base64
    library/cpp/threading/future
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/core/extract_predicate
    yql/essentials/core/file_storage
    yql/essentials/core/services
    yql/essentials/core/url_lister/interface
    yql/essentials/core/url_preprocessing/interface
    yql/essentials/core/credentials
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/qplayer/udf_resolver
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/type_ann
    yql/essentials/providers/common/config
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
    yql/essentials/providers/common/arrow_resolve
    yql/essentials/providers/common/gateways_utils
    yql/essentials/providers/config
    yql/essentials/providers/result/provider
)

YQL_LAST_ABI_VERSION()

END()
