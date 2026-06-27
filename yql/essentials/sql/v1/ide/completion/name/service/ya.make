LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/core
)

END()

RECURSE(
    binding
    cluster
    column
    docs
    impatient
    ranking
    schema
    static
    union
)
