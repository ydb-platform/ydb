LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    fluent_settings_helpers.h
    request_settings.h
    s3_settings.h
    status_codes.h
    ydb.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types
)

END()
