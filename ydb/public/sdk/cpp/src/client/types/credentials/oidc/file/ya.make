LIBRARY()

SRCS(
    from_file.cpp
    token_cache.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/json
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

END()
