LIBRARY()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
)

END()

RECURSE_FOR_TESTS(
    ut
)
