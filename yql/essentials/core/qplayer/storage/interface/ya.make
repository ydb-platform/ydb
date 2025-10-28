LIBRARY()

SRCS(
    yql_qstorage.cpp
)

PEERDIR(
    library/cpp/threading/future
)

GENERATE_ENUM_SERIALIZATION(yql_qstorage.h)

END()
