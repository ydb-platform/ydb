LIBRARY()

ALLOCATOR_IMPL()
SRCS(
    bridge.cpp
)

PEERDIR(
    library/cpp/malloc/api
    library/cpp/yt/containers
    library/cpp/yt/memory
    library/cpp/yt/threading
)

END()
