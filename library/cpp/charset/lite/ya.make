LIBRARY()

SRCDIR(library/cpp/charset)

SRCS(
    generated/cp_data.cpp
    generated/encrec_data.cpp
    codepage.cpp
    cp_encrec.cpp
    doccodes.cpp
    ci_string.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
