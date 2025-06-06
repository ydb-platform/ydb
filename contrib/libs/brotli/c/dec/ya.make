LIBRARY()

VERSION(1.0.1)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(GLOBAL contrib/libs/brotli/c/include)

PEERDIR(
    contrib/libs/brotli/c/common
)

SRCS(
    bit_reader.c
    decode.c
    huffman.c
    state.c
)

END()
