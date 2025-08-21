UNITTEST_FOR(ydb/library/yql/public/issue)

TAG(ya:manual)

FORK_SUBTESTS()

SRCS(
    yql_issue_ut.cpp
    yql_issue_manager_ut.cpp
    yql_issue_utils_ut.cpp
    yql_warning_ut.cpp
)

PEERDIR(
    library/cpp/unicode/normalization
)

END()
