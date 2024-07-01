LIBRARY()

PEERDIR(
    library/cpp/digest/md5
    library/cpp/http/server
    library/cpp/xml/document
    library/cpp/cgiparam
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
)

SRCS(
    s3_mock.cpp
)

END()
