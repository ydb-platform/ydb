LIBRARY()

SRCS(
    resource_pool_classifier_settings.cpp
    resource_pool_settings.cpp
)

PEERDIR(
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
