LIBRARY()

SRCS(
    queries.cpp
    db_queries_maker.cpp
)

PEERDIR(
    ydb/core/ymq/base
)

END()
