LIBRARY()

SRCS(
    quoter.h
    quoter.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/time_series_vec
)

GENERATE_ENUM_SERIALIZATION(quoter.h)

YQL_LAST_ABI_VERSION()

END()
