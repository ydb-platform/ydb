LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
    yql/essentials/sql/v1/complete/core
)

END()

RECURSE(
    ranking
    static
    union
)
