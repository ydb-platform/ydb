YQL_LIBRARY()

IF (ARCH_X86_64)
    PEERDIR(
        yql/essentials/minikql/jsonpath/rewrapper/hyperscan
    )
ENDIF()

PEERDIR(
    library/cpp/json
    yql/essentials/minikql/jsonpath/rewrapper/re2
    yql/essentials/minikql/jsonpath/rewrapper
    yql/essentials/minikql/jsonpath/parser
    yql/essentials/types/binary_json/dom
    yql/essentials/minikql/dom
    yql/essentials/public/issue
    yql/essentials/public/udf
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/public/issue/protos
)

SRCS(
    executor.cpp
    jsonpath.cpp
    value.cpp
)

END()

RECURSE(
    benchmark
    parser
    rewrapper
)

RECURSE_FOR_TESTS(
    ut
)
