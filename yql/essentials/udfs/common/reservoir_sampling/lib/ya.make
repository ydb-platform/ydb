LIBRARY()
PEERDIR(library/cpp/random_provider)

SRCS(
    reservoir.cpp
)

END()

RECURSE_FOR_TESTS(ut)
