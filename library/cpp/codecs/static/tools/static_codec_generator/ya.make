PROGRAM()

OWNER(velavokr)

SRCS(
    static_codec_generator.cpp
)

PEERDIR(
    library/cpp/codecs
    library/cpp/codecs/static
    library/cpp/codecs/static/tools/common
    library/cpp/digest/md5
    library/cpp/getopt/small 
)

END()
