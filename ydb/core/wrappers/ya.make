LIBRARY()

OWNER(
    ilnaz
    g:kikimr
)

IF (OS_WINDOWS)
    CFLAGS( 
        -DKIKIMR_DISABLE_S3_WRAPPER 
    ) 
ELSE()
    SRCS(
        s3_out.cpp
        s3_wrapper.cpp
    )
    PEERDIR(
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
        contrib/libs/curl
        library/cpp/actors/core
        ydb/core/base 
        ydb/core/protos 
        ydb/core/wrappers/ut_helpers 
    )
ENDIF()

END()
 
RECURSE_FOR_TESTS( 
    ut 
    ut_helpers 
) 
