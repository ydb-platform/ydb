LIBRARY()

SRCS(
    object.cpp
    update.cpp
    converter.cpp
    context.cpp
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

YQL_LAST_ABI_VERSION()

END()
