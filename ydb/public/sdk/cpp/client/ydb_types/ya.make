LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/library/grpc/client
    yql/essentials/public/issue
)

GENERATE_ENUM_SERIALIZATION(s3_settings.h)
GENERATE_ENUM_SERIALIZATION(status_codes.h)

END()
