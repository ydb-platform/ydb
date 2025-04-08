LIBRARY()

SRCS(
    frequency.cpp
    json_name_set.cpp
    name_service.cpp
    ranking.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name
    yql/essentials/sql/v1/complete/text
)

RESOURCE(
    yql/essentials/data/language/pragmas_opensource.json pragmas_opensource.json
    yql/essentials/data/language/types.json types.json
    yql/essentials/data/language/sql_functions.json sql_functions.json
    yql/essentials/data/language/udfs_basic.json udfs_basic.json
    yql/essentials/data/language/rules_corr_basic.json rules_corr_basic.json
)

END()

RECURSE_FOR_TESTS(
    ut
)
