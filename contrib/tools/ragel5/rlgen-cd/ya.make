PROGRAM()

NO_UTIL()
NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/tools/ragel5/aapl
    contrib/tools/ragel5/common
    contrib/tools/ragel5/redfsm
)

SRCS(
    fflatcodegen.cpp
    fgotocodegen.cpp
    flatcodegen.cpp
    fsmcodegen.cpp
    ftabcodegen.cpp
    gotocodegen.cpp
    ipgotocodegen.cpp
    main.cpp
    splitcodegen.cpp
    tabcodegen.cpp
)

END()
