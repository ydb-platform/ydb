LIBRARY()

SRCS(
    container.cpp
    range.cpp
    filter.cpp
    predicate.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/core/protos
    ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
