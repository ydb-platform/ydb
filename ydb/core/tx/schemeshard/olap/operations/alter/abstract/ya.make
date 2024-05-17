LIBRARY()

SRCS(
    object.cpp
    update.cpp
    evolution.cpp
    converter.cpp
    context.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/library/accessor
    ydb/core/protos
    ydb/library/actors/wilson
)

YQL_LAST_ABI_VERSION()

END()
