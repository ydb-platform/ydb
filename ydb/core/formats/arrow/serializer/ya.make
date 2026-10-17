YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/services/metadata/abstract
    ydb/library/actors/core
    ydb/library/formats/arrow/common
    ydb/core/protos
)

SRCS(
    abstract.cpp  # gn: into ydb/core/formats/arrow
    GLOBAL native.cpp  # gn: into ydb/core/formats/arrow
    stream.cpp  # gn: into ydb/core/formats/arrow
    parsing.cpp  # gn: into ydb/core/formats/arrow
    utils.cpp  # gn: into ydb/core/formats/arrow
)

END()
