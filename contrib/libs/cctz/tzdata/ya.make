LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2026f)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/libs/cctz
)

INCLUDE(ya.make.resources)

SRCS(
    GLOBAL factory.cpp
)

END()
