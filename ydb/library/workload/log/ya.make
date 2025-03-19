LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    log.cpp
)

PEERDIR(
    ydb/library/workload/abstract
    ydb/public/api/protos
    library/cpp/json
    library/cpp/resource
)

RESOURCE(
    select_queries.sql workload_logs_select_queries.sql

    # Temporary disabled GROUP BY and DISNINCT queries
    # select_queries_original.sql workload_logs_select_queries.sql
)

GENERATE_ENUM_SERIALIZATION(log.h)

END()
