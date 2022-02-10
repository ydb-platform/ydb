LIBRARY()

OWNER(velavokr)

SRCS(
    builder.cpp
    static_codec_info.proto
    static.cpp
)

PEERDIR(
    library/cpp/codecs
    library/cpp/archive
    library/cpp/svnversion
    util/draft
)

END()
