LIBRARY()

SRCS(
    host_mask.cpp
    host_status.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
