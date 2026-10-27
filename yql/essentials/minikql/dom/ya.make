YQL_LIBRARY()

PEERDIR(
    library/cpp/deprecated/enum_codegen
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
