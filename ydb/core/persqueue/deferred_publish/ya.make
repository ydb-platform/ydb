LIBRARY()

SRCS(
    insert_publication_query.cpp
    registry_actor.cpp
    tables_creator.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/services
    ydb/library/persqueue/topic_parser
    ydb/library/table_creator
    ydb/public/lib/scheme_types
    yql/essentials/public/issue
)

END()
