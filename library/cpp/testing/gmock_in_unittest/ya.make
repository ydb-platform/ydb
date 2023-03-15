LIBRARY()

PEERDIR(
    contrib/restricted/googletest/googlemock
    contrib/restricted/googletest/googletest
    library/cpp/testing/gtest_extensions
    library/cpp/testing/unittest
)

SRCS(
    events.cpp
    GLOBAL registration.cpp
)

END()

RECURSE_FOR_TESTS(
    example_ut
)
