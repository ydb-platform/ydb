LIBRARY()

SRCS(
    position.cpp
    throttle.cpp
    throttled.cpp
    buffered_throttled_file.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
