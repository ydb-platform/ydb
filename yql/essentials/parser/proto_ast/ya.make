LIBRARY()

HEADERS(common.h)

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    contrib/libs/protobuf
    yql/essentials/parser/common
)

END()

RECURSE(
    antlr3
    antlr4
    collect_issues
    gen
)
