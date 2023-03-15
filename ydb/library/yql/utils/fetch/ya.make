LIBRARY()

SRCS(
    fetch.cpp
)

PEERDIR(
    library/cpp/charset
    library/cpp/http/io
    library/cpp/http/misc
    library/cpp/openssl/io
    library/cpp/uri
    ydb/library/yql/utils/log
)

END()
