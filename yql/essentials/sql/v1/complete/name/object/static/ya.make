LIBRARY()

SRCS(
    schema_gateway.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/object
)

END()

RECURSE_FOR_TESTS(
    ut
)
