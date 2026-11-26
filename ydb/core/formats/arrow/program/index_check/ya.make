LIBRARY()

SRCS(
    index_check.h
)

PEERDIR(
    ydb/library/accessor
)

GENERATE_ENUM_SERIALIZATION(index_check.h)

END()
