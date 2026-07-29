LIBRARY(library-formats-arrow-accessor-composite)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/filter
    ydb/library/formats/arrow/common
)

SRCS(
    accessor.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
