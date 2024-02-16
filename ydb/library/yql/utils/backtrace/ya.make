LIBRARY()

SRCS(
    backtrace.cpp
    symbolize.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
    ydb/library/yql/utils/backtrace/libbacktrace
)

END()

RECURSE_FOR_TESTS(
    ut
)

