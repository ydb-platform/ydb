LIBRARY()

SRCS(
    schema_gateway.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/object
    yql/essentials/sql/v1/complete/name/object/simple
)

END()

RECURSE_FOR_TESTS(
    ut
)
