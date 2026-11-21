LIBRARY()

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/digest/md5
    library/cpp/http/server
    library/cpp/xml/document
    library/cpp/cgiparam
    ydb/core/wrappers
    ydb/library/actors/core
)

SRCS(
    fs_mock.cpp
    s3_mock.cpp
    s3_mock_storage.cpp
)

END()
