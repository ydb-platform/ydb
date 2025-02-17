LIBRARY()

SRCS(
    yql_yt_url_mapper.cpp
)

PEERDIR(
    yql/essentials/providers/common/proto
    library/cpp/regex/pcre
    library/cpp/uri
    library/cpp/cgiparam
)

END()
