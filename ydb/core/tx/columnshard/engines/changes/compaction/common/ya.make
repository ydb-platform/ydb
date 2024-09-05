LIBRARY()

SRCS(
    context.cpp
    result.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
)

END()
