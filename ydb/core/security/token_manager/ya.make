LIBRARY()

SRCS(
    token_manager.cpp
    vm_metadata_token_provider_handler.cpp
    token_provider.cpp
)


PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/core/protos
    ydb/core/util
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
