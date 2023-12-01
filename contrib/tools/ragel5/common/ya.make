LIBRARY()

LICENSE(GPL-2.0-or-later)

NO_UTIL()
NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/tools/ragel5/common
)

PEERDIR(
    contrib/tools/ragel5/aapl
)

SRCS(
    common.cpp
)

END()
