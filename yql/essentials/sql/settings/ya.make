LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
    translator.cpp
)

PEERDIR(
    yql/essentials/core/project_settings
    library/cpp/deprecated/split
    library/cpp/json
    yql/essentials/public/issue
    yql/essentials/public/langver
    yql/essentials/public/sql_meta
    yql/essentials/public/udf_meta
    yql/essentials/core/issue
    yql/essentials/core/pg_settings
    yql/essentials/public/issue/protos
    yql/essentials/sql/settings/flags
    yql/essentials/utils
)

GENERATE_ENUM_SERIALIZATION(translation_settings.h)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_split_code/lib
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
)

END()

RECURSE(
    flags
)

RECURSE_FOR_TESTS(
    ut
)
