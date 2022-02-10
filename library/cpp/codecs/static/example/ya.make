LIBRARY()

OWNER(velavokr)

SRCS(
    GLOBAL example.cpp
)

PEERDIR(
    library/cpp/codecs
    library/cpp/codecs/static
)

ARCHIVE_ASM(
    "solar-8k-a.huffman.1467494385.codec_info"
    NAME codec_info_sa_huff_20160707
)

ARCHIVE_ASM(
    "huffman.1467494385.codec_info"
    NAME codec_info_huff_20160707
)

END()
