LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    log_writer.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(log_writer.h)

END()
