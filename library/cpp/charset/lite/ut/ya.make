UNITTEST_FOR(library/cpp/charset/lite)

SRCDIR(library/cpp/charset)

CFLAGS(-DLIBRARY_CHARSET_WITHOUT_LIBICONV=yes)
SRCS(
    ci_string_ut.cpp
    codepage_ut.cpp
)

END()
