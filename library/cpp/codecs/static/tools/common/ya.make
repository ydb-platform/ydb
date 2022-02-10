LIBRARY()

OWNER(velavokr)

SRCS(
    ct_common.cpp
)

PEERDIR(
    library/cpp/codecs 
    library/cpp/codecs/static 
    library/cpp/getopt/small
    library/cpp/string_utils/base64
    util/draft
)

GENERATE_ENUM_SERIALIZATION(ct_common.h)

END()
