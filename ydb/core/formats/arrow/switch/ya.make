LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/scheme_types
    ydb/library/actors/core
)

SRCS(
    switch_type.cpp
    compare.cpp
)

END()
