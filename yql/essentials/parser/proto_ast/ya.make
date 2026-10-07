LIBRARY()

HEADERS(common.h)

PEERDIR(
    contrib/libs/protobuf
    yql/essentials/parser/common
)

END()

RECURSE(
    antlr4
    collect_issues
    gen
)
