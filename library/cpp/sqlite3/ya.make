LIBRARY()

SRCS(
    sqlite.cpp
)

PEERDIR(
    contrib/libs/sqlite3
)

END()

RECURSE_FOR_TESTS(ut)
