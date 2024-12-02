LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson
    yt/yql/providers/yt/common
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/skiff
    yql/essentials/utils
)

END()
