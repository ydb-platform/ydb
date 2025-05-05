LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    vector.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector.h)

END()
