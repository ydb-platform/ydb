LIBRARY()

PEERDIR(
    contrib/libs/protobuf
)

SRCS(
    common.cpp
)

END()

RECURSE(
    antlr3
    antlr4
    collect_issues
    gen
)
