LIBRARY()

PEERDIR(
    library/cpp/threading/future
)

SRCS(
    cancellation_token.cpp
)

END()

RECURSE_FOR_TESTS(ut)
