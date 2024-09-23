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
    ydb/core/formats/arrow
    ydb/core/formats/arrow/protos
)

YQL_LAST_ABI_VERSION()

END()
