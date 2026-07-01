LIBRARY()

SRCS(
    mon_render.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
    ut
)
