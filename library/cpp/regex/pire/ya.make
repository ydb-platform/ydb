LIBRARY()

CFLAGS(-DPIRE_NO_CONFIG)

SRCDIR(contrib/libs/pire/pire)

SRCS(
    pcre2pire.cpp
    classes.cpp
    encoding.cpp
    fsm.cpp
    scanner_io.cpp
    easy.cpp
    scanners/null.cpp
    extra/capture.cpp
    extra/count.cpp
    extra/glyphs.cpp
    re_lexer.cpp
    re_parser.y
    read_unicode.cpp
    extraencodings.cpp
    approx_matching.cpp
    half_final_fsm.cpp
    minimize.h
)

PEERDIR(
    library/cpp/charset
)

END()

RECURSE_FOR_TESTS(ut)
