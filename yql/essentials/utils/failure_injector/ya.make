LIBRARY()

SRCS(
    failure_injector.cpp
)

PEERDIR(
    yql/essentials/utils
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
