LIBRARY()

SRCDIR(library/cpp/charset)

SRCS(
    generated/cp_data.cpp
    generated/encrec_data.cpp
    codepage.cpp
    codepage.h
    cp_encrec.cpp
    doccodes.cpp
    doccodes.h
    ci_string.cpp
    ci_string.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
