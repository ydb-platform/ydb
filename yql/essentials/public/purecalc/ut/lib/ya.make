LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/yson
    library/cpp/yson/node
)

SRCS(
    helpers.cpp
    helpers.h
)

END()
