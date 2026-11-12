YQL_LIBRARY()

SRCS(
    json_index.cpp
)

PEERDIR(
    library/cpp/json
    yql/essentials/public/issue
    yql/essentials/public/udf
    yql/essentials/minikql/jsonpath/parser
    yql/essentials/types/binary_json
)

END()

RECURSE_FOR_TESTS(
    ut
)
