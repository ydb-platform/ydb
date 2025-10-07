LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    bind_in_range.cpp
    bind_in_range.h
)

PEERDIR(
    library/cpp/messagebus
)

END()
