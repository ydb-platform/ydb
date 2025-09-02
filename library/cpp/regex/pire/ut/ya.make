# this test in not linked into build tree with ReCURSE and is built by unittest/library

UNITTEST()

PEERDIR(
    library/cpp/regex/pire
)

ADDINCL(
    library/cpp/regex/pire/pire
    library/cpp/regex/pire/ut
)

SRCS(
    pire_ut.cpp
    capture_ut.cpp
    count_ut.cpp
    glyph_ut.cpp
    easy_ut.cpp
    read_unicode_ut.cpp
    regexp_ut.cpp
    approx_matching_ut.cpp
)

SIZE(MEDIUM)

TIMEOUT(600)

PIRE_INLINE(inline_ut.cpp)

END()
