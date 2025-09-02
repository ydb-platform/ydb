LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(libpng-2.0)

PEERDIR(
    contrib/libs/libpng
)

ADDINCL(
    GLOBAL contrib/libs/libpng/include
)

END()
