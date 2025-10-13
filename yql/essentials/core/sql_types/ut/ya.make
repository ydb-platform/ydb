UNITTEST_FOR(yql/essentials/core/sql_types)

ENABLE(YQL_STYLE_CPP)

SRCS(
    match_recognize_ut.cpp
    normalize_name_ut.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
)

SIZE(SMALL)

END()
