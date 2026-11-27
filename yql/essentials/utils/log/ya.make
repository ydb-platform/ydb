LIBRARY()

SRCS(
    context.cpp
    format.cpp
    fwd_backend.cpp
    log.cpp
    profile.cpp
    tls_backend.cpp
    log_component.cpp
    log_level.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/logger
    library/cpp/logger/global
    library/cpp/deprecated/atomic
    library/cpp/json
    yql/essentials/utils/log/proto
    yql/essentials/utils/backtrace
    yql/essentials/utils
)

PEERDIR_FROM_POLICY(
    ALLOW
    quality/relev_tools/personalization/user_history/antifraud/udf/library
)

END()

RECURSE_FOR_TESTS(
    ut
)
