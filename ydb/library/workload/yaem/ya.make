LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    yaem.cpp
)

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/log
)

END()

RECURSE_FOR_TESTS(ut)
