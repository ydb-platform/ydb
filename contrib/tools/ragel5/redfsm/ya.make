LIBRARY()

LICENSE(GPL-2.0-or-later)

NO_UTIL()
NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/tools/ragel5/redfsm
)

PEERDIR(
    contrib/tools/ragel5/aapl
    contrib/tools/ragel5/common
)

SRCS(
    gendata.cpp
    redfsm.cpp
    xmlparse.cpp
    xmlscan.cpp
    xmltags.cpp
)

END()
