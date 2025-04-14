LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    mixed.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(mixed.h)

END()
