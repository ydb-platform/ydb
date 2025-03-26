LIBRARY()

SRCS(
    yql_completer.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/complete
)

END()

RECURSE_FOR_TESTS(
    ut
)
