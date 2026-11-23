YQL_LIBRARY()

SRCS(kqp_eval.cpp)

PEERDIR(
    contrib/restricted/abseil-cpp
    yql/essentials/ast
    yql/essentials/public/decimal
    yql/essentials/public/udf
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(ut)
