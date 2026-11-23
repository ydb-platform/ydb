LIBRARY()

SRCS(
    convert.cpp
)

PEERDIR(
    library/cpp/json/yson
    library/cpp/yson/node
    yql/essentials/core/project_settings
    yql/essentials/core/project_settings/proto
    yql/essentials/utils
    yql/essentials/public/langver
)

END()
