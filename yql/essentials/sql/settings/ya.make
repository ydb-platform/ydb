LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/json
    yql/essentials/core/issue
    yql/essentials/core/pg_settings
    yql/essentials/core/issue/protos
    yql/essentials/utils
)

END()
