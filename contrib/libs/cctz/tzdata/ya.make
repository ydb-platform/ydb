LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/libs/cctz
)

INCLUDE(ya.make.resources)

SRCS(
    GLOBAL factory.cpp
)

END()
