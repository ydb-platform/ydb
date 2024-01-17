LIBRARY()

SRCS(
    parser.rl6
    http.cpp
    types.cpp
    xml.cpp
    xml_builder.cpp
)

PEERDIR(
    contrib/libs/libxml
    ydb/library/actors/core
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/protobuf/json
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/string_utils/url
    ydb/core/protos
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/library/http_proxy/authorization
    ydb/library/http_proxy/error
)

END()

RECURSE_FOR_TESTS(
    ut
)
