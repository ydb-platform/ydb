LIBRARY()

SRCS(
    quoter.h
    quoter.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/time_series_vec
)

GENERATE_ENUM_SERIALIZATION(quoter.h)

END()
