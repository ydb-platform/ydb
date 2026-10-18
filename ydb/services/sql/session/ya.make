LIBRARY()

SRCS(
    session.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/sql/datashard
    ydb/services/sql/ddl
    ydb/services/sql/ischema
    ydb/services/sql/parse
)

END()
