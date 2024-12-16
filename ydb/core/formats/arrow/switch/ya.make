LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/scheme_types
    ydb/library/actors/core
    ydb/library/formats/arrow/switch
)

SRCS(
    switch_type.cpp
)

END()
