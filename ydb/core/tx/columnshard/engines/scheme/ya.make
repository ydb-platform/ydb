LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    index_info.cpp
    tier_info.cpp
    column_features.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow

    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
