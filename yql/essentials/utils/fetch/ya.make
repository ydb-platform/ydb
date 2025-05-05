LIBRARY()

SRCS(
    fetch.cpp
)

PEERDIR(
    library/cpp/charset/lite
    library/cpp/http/io
    library/cpp/http/misc
    library/cpp/openssl/io
    library/cpp/uri
    yql/essentials/utils/log
)

END()
