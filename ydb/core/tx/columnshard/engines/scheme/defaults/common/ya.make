YQL_LIBRARY()

SRCS(
    scalar.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/core/formats/arrow
    ydb/core/scheme_types
    ydb/library/actors/core
    ydb/library/formats/arrow
    ydb/public/api/protos
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
)

END()
