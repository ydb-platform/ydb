RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    constructor.cpp
    collection.cpp
    header.cpp
    fetcher.cpp
    abstract.cpp
    meta.cpp
    checker.cpp
    GLOBAL composite.cpp
    simple.cpp
    tree.cpp
    coverage.cpp
    like.cpp
    common.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/formats/arrow/protos
    yql/essentials/core/arrow_kernels/request
    ydb/core/formats/arrow/program
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
