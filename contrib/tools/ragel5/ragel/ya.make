PROGRAM(ragel5)

NO_UTIL()
NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/tools/ragel5/aapl
    contrib/tools/ragel5/common
)

SRCS(
    fsmap.cpp
    fsmattach.cpp
    fsmbase.cpp
    fsmgraph.cpp
    fsmmin.cpp
    fsmstate.cpp
    main.cpp
    parsedata.cpp
    parsetree.cpp
    rlparse.cpp
    rlscan.cpp
    xmlcodegen.cpp
)

END()
