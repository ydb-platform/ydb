LIBRARY()

YQL_ABI_VERSION(2 28 0)

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/json
    library/cpp/yson_pull
    yql/essentials/public/udf
    yql/essentials/utils
)

SRCS(
    node.cpp
    json.cpp
    yson.cpp
    make.cpp
    peel.cpp
    hash.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
