LIBRARY()

PEERDIR(
    library/cpp/digest/md5
    library/cpp/http/server
    library/cpp/xml/document
    library/cpp/cgiparam
)

SRCS(
    fs_mock.cpp
    s3_mock.cpp
)

END()
