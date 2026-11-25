LIBRARY()

PEERDIR(
    library/cpp/random_provider
)

SRCS(
    random_data_generator.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
