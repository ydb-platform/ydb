LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    split_merge.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

END()
