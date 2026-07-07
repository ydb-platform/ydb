LIBRARY()

SRCS(
    environment.cpp
    input.cpp
    name.cpp
    position.cpp
    statement.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/pure_ast
    yql/essentials/core/sql_types
    library/cpp/yson/node
)

END()
