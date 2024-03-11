LIBRARY()

SRCS(
    constructor.cpp
    meta.cpp
    checker.cpp
    program.cpp
    GLOBAL composite.cpp
    simple.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
