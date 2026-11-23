LIBRARY()

PEERDIR(
    library/cpp/yson/node
    yql/essentials/utils/meta
)

SRCS(
    environment.cpp
    format.cpp
    position.cpp
)

END()
