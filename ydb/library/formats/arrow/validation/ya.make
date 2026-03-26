LIBRARY(library-formats-arrow-validation)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/library/actors/core
)

SRCS(
    validation.cpp
)

END()
