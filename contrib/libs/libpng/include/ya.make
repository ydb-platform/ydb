LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(libpng-2.0)

PEERDIR(
    contrib/libs/libpng
)

ADDINCL(
    GLOBAL contrib/libs/libpng/include
)

END()
