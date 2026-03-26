LIBRARY()

SRCS(
    scalar.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/libs/apache/arrow_next
    ydb/library/conclusion
    ydb/core/scheme_types
    ydb/library/actors/core
)

END()
