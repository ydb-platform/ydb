UNITTEST()

FORK_SUBTESTS()

SRCS(
    utf8_ut.cpp
    yql_issue_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
    ydb/public/sdk/cpp/src/library/issue
)

END()
