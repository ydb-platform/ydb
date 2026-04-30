LIBRARY()

GENERATE_ENUM_SERIALIZATION(ddisk_state.h)

SRCS(
    ddisk_state.cpp
    host_mask.cpp
    host_status.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
