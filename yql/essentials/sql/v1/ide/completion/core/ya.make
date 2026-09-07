LIBRARY()

SRCS(
    input.cpp
    name.cpp
    statement.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/core
    yql/essentials/sql/v1/ide/pure_ast
    yql/essentials/core/sql_types
)

END()
