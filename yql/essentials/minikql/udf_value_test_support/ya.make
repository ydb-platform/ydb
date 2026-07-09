LIBRARY()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/utils/random_data_generator
)

END()

RECURSE_FOR_TESTS(
    ut
)
