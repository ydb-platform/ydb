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
    ydb/core/tx/schemeshard/olap/operations/alter/protos

)

YQL_LAST_ABI_VERSION()

END()
