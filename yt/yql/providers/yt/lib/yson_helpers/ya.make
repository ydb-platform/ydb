LIBRARY()

SRCS(
    yson_helpers.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/utils
    yql/essentials/utils/log
    yt/yql/providers/yt/common
)

END()
