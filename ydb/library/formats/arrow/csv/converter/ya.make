LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    yql/essentials/types/uuid
)

END()
