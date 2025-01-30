LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    log.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(log.h)

END()
