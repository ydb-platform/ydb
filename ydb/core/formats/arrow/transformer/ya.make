LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/dictionary
)

SRCS(
    abstract.cpp
    dictionary.cpp
    composite.cpp
)

END()
