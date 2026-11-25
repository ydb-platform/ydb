LIBRARY()

PEERDIR(
    library/cpp/getopt/small
    library/cpp/svnversion
    library/cpp/build_info
)

SRCS(
    GLOBAL print.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/fastops
)

END()

RECURSE(
    last_getopt_demo
    small
    ut
)
