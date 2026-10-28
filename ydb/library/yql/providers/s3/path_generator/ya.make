YQL_LIBRARY()

SRCS(
    yql_s3_path_generator.cpp
)

PEERDIR(
    library/cpp/scheme
    yql/essentials/minikql/computation
    yql/essentials/minikql/datetime
    yql/essentials/public/udf
)

GENERATE_ENUM_SERIALIZATION(yql_s3_path_generator.h)

END()

RECURSE_FOR_TESTS(
    ut
)
