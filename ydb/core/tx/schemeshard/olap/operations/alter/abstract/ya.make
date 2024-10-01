LIBRARY()

SRCS(
    object.cpp
    update.cpp
    converter.cpp
    context.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/library/accessor
    ydb/core/protos
    ydb/library/actors/wilson
    ydb/library/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
