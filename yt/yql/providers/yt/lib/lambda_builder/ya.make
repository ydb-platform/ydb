LIBRARY()

SRCS(
    lambda_builder.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    yql/essentials/ast
    yql/essentials/minikql/computation
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
