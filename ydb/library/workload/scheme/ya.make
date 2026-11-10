LIBRARY()

SRCS(
    config.cpp
    runner.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/threading/local_executor
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(ut)
