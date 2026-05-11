LIBRARY()

SRCS(
    yql_poly_args.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/public/langver
)

END()

RECURSE_FOR_TESTS(
    ut
)

