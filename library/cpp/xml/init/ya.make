LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/libxml
    library/cpp/charset
)

SRCS(
    ptr.cpp
    init.cpp
)

END()
