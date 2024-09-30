LIBRARY(library-formats-arrow-switch)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
)

SRCS(
    switch_type.cpp
    compare.cpp
)

END()
