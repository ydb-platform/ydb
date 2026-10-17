LIBRARY()

# gn: move into parent

PEERDIR()

SRCS(
    account_lockout.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
