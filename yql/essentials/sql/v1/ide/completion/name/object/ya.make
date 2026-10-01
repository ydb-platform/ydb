LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/utils/meta
)

END()

RECURSE(
    simple
)
