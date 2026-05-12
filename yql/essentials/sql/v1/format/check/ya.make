LIBRARY()

SRCS(
    check_format.cpp
)

PEERDIR(
    yql/essentials/sql/v1/format
)

END()

RECURSE_FOR_TESTS(ut)
