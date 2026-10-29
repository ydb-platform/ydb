UNITTEST_FOR(ydb/library/naming_conventions)

FORK_SUBTESTS()
SPLIT_FACTOR(2)

SRCS(
    naming_conventions_ut.cpp
    scheme_object_names_ut.cpp
)

END()
