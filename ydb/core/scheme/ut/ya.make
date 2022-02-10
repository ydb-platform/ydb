UNITTEST_FOR(ydb/core/scheme) 

OWNER(g:kikimr)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/scheme 
)

SRCS(
    scheme_borders_ut.cpp
    scheme_tablecell_ut.cpp
)

END()
