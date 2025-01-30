LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    stock.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(stock.h)

END()
