LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    cbo_simple.cpp
)

PEERDIR(
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/utils
)

END()
