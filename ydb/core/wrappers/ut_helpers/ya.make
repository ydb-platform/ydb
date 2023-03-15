LIBRARY()

PEERDIR(
    library/cpp/digest/md5
    library/cpp/http/server
    library/cpp/xml/document
    library/cpp/cgiparam
)

SRCS(
    s3_mock.cpp
)

END()
