LIBRARY()

SRCS(
    timeout.cpp
    ss_dialog.cpp
)

PEERDIR(
    library/cpp/time_provider
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/library/services
    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
)

YQL_LAST_ABI_VERSION()

END()
