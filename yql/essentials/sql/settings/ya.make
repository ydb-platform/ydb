LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
    translation_sql_flags.cpp
    translator.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/json
    yql/essentials/public/issue
    yql/essentials/public/langver
    yql/essentials/public/udf_meta
    yql/essentials/core/issue
    yql/essentials/core/pg_settings
    yql/essentials/public/issue/protos
    yql/essentials/utils
)

END()
