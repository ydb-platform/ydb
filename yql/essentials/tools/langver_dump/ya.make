PROGRAM()

ENABLE(YQL_STYLE_CPP)

SRCS(
    langver_dump.cpp
)

PEERDIR(
    yql/essentials/public/langver
    yql/essentials/utils/backtrace
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
