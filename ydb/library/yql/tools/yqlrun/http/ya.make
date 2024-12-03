LIBRARY()

SRCS(
    assets_servlet.cpp
    server.cpp
    servlet.cpp
    yql_functions_servlet.cpp
    yql_servlet.cpp
    yql_server.cpp
)

PEERDIR(
    library/cpp/charset
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/json
    library/cpp/logger
    library/cpp/mime/types
    library/cpp/openssl/io
    library/cpp/string_utils/quote
    library/cpp/uri
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/core/facade
    yql/essentials/core/type_ann
    ydb/library/yql/dq/opt
    ydb/library/yql/providers/dq/helper
    ydb/library/yql/providers/dq/provider
    yql/essentials/providers/result/provider
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/v1/format
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/provider
    yql/essentials/core/url_preprocessing
    yql/essentials/providers/pg/provider
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

YQL_LAST_ABI_VERSION()

END()
