LIBRARY()

SRCS(
    vocabulary.cpp
    c3i.cpp
    c3t.cpp
    defs.cpp
    pipeline.cpp
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    contrib/libs/antlr4-c3
    yql/essentials/sql/v1/ide/completion/core
    yql/essentials/sql/v1/ide/completion/text
)

END()
