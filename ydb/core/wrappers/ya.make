LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_WRAPPER
    )
ELSE()
    SRCS(
        s3_wrapper.cpp
        s3_storage.cpp
        s3_storage_config.cpp
        abstract.cpp
        fake_storage.cpp
        fake_storage_config.cpp
    )
    PEERDIR(
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
        contrib/libs/curl
        ydb/library/actors/core
        ydb/core/base
        ydb/core/protos
        ydb/core/wrappers/events
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
    ut_helpers
)
