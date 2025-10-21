LIBRARY()

SRCS(
    pcre2pire.cpp
    extraencodings.cpp
    pire/classes.cpp
    pire/encoding.cpp
    pire/fsm.cpp
    pire/scanner_io.cpp
    pire/easy.cpp
    pire/scanners/null.cpp
    pire/extra/capture.cpp
    pire/extra/count.cpp
    pire/extra/glyphs.cpp
    pire/re_lexer.cpp
    pire/re_parser.y
    pire/read_unicode.cpp
    pire/approx_matching.cpp
    pire/half_final_fsm.cpp
)

PEERDIR(
    library/cpp/charset
)

END()

RECURSE_FOR_TESTS(ut)
