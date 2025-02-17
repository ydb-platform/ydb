LIBRARY()

SRCS(
    udf_tz.cpp
    udf_tz.h
)

PEERDIR(
)

PROVIDES(YqlUdfTz)

END()

RECURSE(
    gen
)

RECURSE_FOR_TESTS(
    ut
)
