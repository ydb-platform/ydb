LIBRARY()

SRCS(subsystem.cpp)

PEERDIR(ydb/library/actors/core)

END()

RECURSE(real mock)

RECURSE_FOR_TESTS(ut)
