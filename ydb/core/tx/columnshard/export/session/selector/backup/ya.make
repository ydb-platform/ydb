LIBRARY()

SRCS(
    GLOBAL selector.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/selector/abstract
    ydb/core/protos
    ydb/library/yql/dq/actors/protos
)

END()
