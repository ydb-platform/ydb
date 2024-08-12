LIBRARY()

SRCS(
    scalar.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/core/scheme_types
)

END()
