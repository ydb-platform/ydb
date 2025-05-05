LIBRARY()

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
