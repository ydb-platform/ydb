LIBRARY()

SRCS(
    assets_servlet.cpp
    server.cpp
    servlet.cpp
    sql_tokens_servlet.cpp
    yql_functions_servlet.cpp
    yql_servlet.cpp
    yql_server.cpp
)

PEERDIR(
    yt/yql/providers/yt/common
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/provider

    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/pg/provider
    yql/essentials/providers/config
    yql/essentials/providers/result/provider

    yql/essentials/public/issue
    yql/essentials/core/facade
    yql/essentials/core/url_preprocessing
    yql/essentials/core/peephole_opt
    yql/essentials/core/type_ann
    yql/essentials/core/cbo/simple
    yql/essentials/core/services
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/utils/log
    yql/essentials/utils

    library/cpp/http/io
    library/cpp/http/server
    library/cpp/http/misc
    library/cpp/mime/types
    library/cpp/uri
    library/cpp/logger
    library/cpp/yson/node
    library/cpp/openssl/io
    library/cpp/charset/lite
    library/cpp/yson
    library/cpp/json
    library/cpp/string_utils/quote
    library/cpp/getopt

    contrib/libs/protobuf
)

FILES(
    www/bower.json
    www/favicon.ico
    www/file-index.html
    www/css/base.css
    www/js/ace.min.js
    www/js/app.js
    www/js/dagre-d3.core.min.js
    www/js/dagre.core.min.js
    www/js/graphlib.core.min.js
    www/js/mode-sql.js
    www/js/mode-yql.js
    www/js/theme-tomorrow.min.js
)

RESOURCE(
    yql/essentials/data/language/types.json types.json
    yql/essentials/data/language/sql_functions.json sql_functions.json
)

YQL_LAST_ABI_VERSION()

END()
