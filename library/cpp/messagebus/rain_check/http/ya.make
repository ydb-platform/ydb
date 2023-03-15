LIBRARY()

SRCS(
    client.cpp
    http_code_extractor.cpp
)

PEERDIR(
    library/cpp/messagebus/rain_check/core
    library/cpp/neh
    library/cpp/http/misc
    library/cpp/http/io
)

END()
