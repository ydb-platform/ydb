LIBRARY()

SRCS(
    params.cpp
    parser.rl6
    xml_builder.cpp
)

PEERDIR(
    contrib/libs/libxml
    ydb/library/actors/core
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/protobuf/json
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/string_utils/url
    ydb/core/protos
    ydb/core/ymq/base
    ydb/library/http_proxy/authorization
    ydb/library/http_proxy/error
)

END()

RECURSE_FOR_TESTS(
    ut
)
