LIBRARY()

SRCS(
    aws.h
)

IF (OS_WINDOWS)
    SRCS(
        aws_windows_stub.cpp
    )
ELSE()
    PEERDIR(
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
        contrib/libs/curl
    )
    SRCS(
        aws.cpp
    )
ENDIF()

END()
