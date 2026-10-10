UNITTEST()

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
SRCS(
    utf8_ut.cpp
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    ydb/public/sdk/cpp/src/library/issue
)

END()
