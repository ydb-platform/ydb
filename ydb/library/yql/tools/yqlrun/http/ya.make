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
    ydb/library/yql/core/facade
    ydb/library/yql/core/type_ann
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/result/provider
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/v1/format
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/core/url_preprocessing
)

YQL_LAST_ABI_VERSION()

END()
