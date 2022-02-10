LIBRARY()

OWNER(g:passport_infra)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    library/cpp/openssl/crypto
    library/cpp/streams/brotli
    library/cpp/streams/zstd
    library/cpp/string_utils/quote
    library/cpp/tvmauth
    library/cpp/tvmauth/client/misc/retry_settings/v1
)

SRCS(
    client_status.cpp
    facade.cpp
    logger.cpp
    misc/api/roles_fetcher.cpp
    misc/api/settings.cpp
    misc/api/threaded_updater.cpp
    misc/async_updater.cpp
    misc/disk_cache.cpp
    misc/last_error.cpp
    misc/proc_info.cpp
    misc/roles/decoder.cpp
    misc/roles/entities_index.cpp
    misc/roles/parser.cpp
    misc/roles/roles.cpp
    misc/threaded_updater.cpp
    misc/tool/meta_info.cpp
    misc/tool/settings.cpp
    misc/tool/threaded_updater.cpp
    misc/utils.cpp
    mocked_updater.cpp
)

GENERATE_ENUM_SERIALIZATION(client_status.h)
GENERATE_ENUM_SERIALIZATION(misc/async_updater.h)
GENERATE_ENUM_SERIALIZATION(misc/last_error.h)

END()

RECURSE_FOR_TESTS(
    examples
    misc/api/dynamic_dst
    ut
)
