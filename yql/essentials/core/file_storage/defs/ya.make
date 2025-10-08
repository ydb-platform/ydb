LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    downloader.cpp
    provider.cpp
)

PEERDIR(
    library/cpp/uri
)

END()
