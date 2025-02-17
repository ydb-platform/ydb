LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/expr
    yt/yql/providers/yt/common
)

END()
