LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_WRAPPER
    )
ELSE()
    SRCS(
        common.cpp
        list_objects.cpp
        object_exists.cpp
        delete_objects.cpp
        get_object.cpp
        s3_out.cpp
        abstract.cpp
    )
    PEERDIR(
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
        contrib/libs/curl
        ydb/library/actors/core
        ydb/core/base
        ydb/core/protos
        ydb/core/wrappers/ut_helpers
    )
ENDIF()

END()
