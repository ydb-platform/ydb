LIBRARY(library-formats-arrow-simple_builder)

PEERDIR(
    contrib/libs/apache/arrow_next
)

SRCS(
    filler.cpp
    array.cpp
    batch.cpp
)

END()
