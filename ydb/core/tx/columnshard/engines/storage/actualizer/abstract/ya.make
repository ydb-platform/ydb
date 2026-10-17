LIBRARY()

# gn: move into parent

SRCS(
    abstract.cpp
    context.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
