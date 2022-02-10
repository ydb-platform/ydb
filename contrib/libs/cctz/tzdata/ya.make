LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0) 

OWNER(
    dfyz
    petrk
)

PEERDIR(
    contrib/libs/cctz
)

INCLUDE(ya.make.resources)

SRCS(
    GLOBAL factory.cpp
)

END()
