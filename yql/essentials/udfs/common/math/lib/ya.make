LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    erfinv.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
