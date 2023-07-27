LIBRARY()

SRCS(
    udf_tz.cpp
    udf_tz.h
)

PEERDIR(
)

PROVIDES(YqlUdfTz)

END()

RECURSE_FOR_TESTS(
    ut
)
