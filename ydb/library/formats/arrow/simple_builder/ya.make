LIBRARY(library-formats-arrow-simple_builder)

PEERDIR(
    contrib/libs/apache/arrow
)

SRCS(
    filler.cpp
    array.cpp
    batch.cpp
)

END()
