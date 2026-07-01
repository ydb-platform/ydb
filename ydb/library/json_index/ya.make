LIBRARY()

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

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
