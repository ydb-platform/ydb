LIBRARY()

SRCS(
    client_provider.cpp
    config.cpp
    credentials.cpp
    device_provider.cpp
    factory.cpp
    protocol.cpp
    provider.cpp
    static_provider.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/simple
    library/cpp/json
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/uri
    library/cpp/yaml/as
    ydb/public/sdk/cpp/src/client/types/core_facility
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
