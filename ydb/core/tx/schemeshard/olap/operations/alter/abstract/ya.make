YQL_LIBRARY()

SRCS(
    object.cpp  # gn: into ydb/core/tx/schemeshard
    update.cpp  # gn: into ydb/core/tx/schemeshard
    converter.cpp  # gn: into ydb/core/tx/schemeshard
    context.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/library/accessor
    ydb/core/protos
    ydb/library/actors/wilson
    ydb/library/formats/arrow
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
