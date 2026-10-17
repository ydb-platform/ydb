YQL_LIBRARY()

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/abstract
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
)

SRCS(
    accessor.cpp  # gn: into ydb/core/formats/arrow
    GLOBAL constructor.cpp  # gn: into ydb/core/formats/arrow
    GLOBAL request.cpp  # gn: into ydb/core/formats/arrow
)

END()
