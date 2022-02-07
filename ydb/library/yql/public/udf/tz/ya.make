LIBRARY()

OWNER(vvvv g:yql_ydb_core)

SRCS(
    udf_tz.cpp
    udf_tz.h
)

PEERDIR(
)

END()

RECURSE_FOR_TESTS(
    ut
)
