G_BENCHMARK()

OWNER(g:balancer)

SRCS(
    alloc_bm.cpp
)

PEERDIR(
    library/cpp/coroutine/engine
)

END()