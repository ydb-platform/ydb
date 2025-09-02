LIBRARY()

SRCS(
    dummy.cpp
    frequency.cpp
    ranking.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/service
)

RESOURCE(
    yql/essentials/data/language/rules_corr_basic.json rules_corr_basic.json
)

END()

RECURSE_FOR_TESTS(
    ut
)
