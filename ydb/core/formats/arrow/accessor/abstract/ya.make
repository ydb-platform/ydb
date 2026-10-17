YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    ydb/library/conclusion
    ydb/services/metadata/abstract
    ydb/library/actors/core
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/common
    ydb/core/formats/arrow/filter
    ydb/library/formats/arrow/protos
    ydb/library/arrow_kernels
)

SRCS(
    common.cpp  # gn: into ydb/core/formats/arrow
    constructor.cpp  # gn: into ydb/core/formats/arrow
    request.cpp  # gn: into ydb/core/formats/arrow
    accessor.cpp  # gn: into ydb/core/formats/arrow
    minmax_utils.cpp  # gn: into ydb/core/formats/arrow
)

GENERATE_ENUM_SERIALIZATION(accessor.h)

END()
