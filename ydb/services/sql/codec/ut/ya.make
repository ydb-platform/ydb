UNITTEST()

SIZE(SMALL)

SRCS(
    decimal_ut.cpp
    ints_ut.cpp
    text_ut.cpp
    time_ut.cpp
)

PEERDIR(
    ydb/services/sql/codec
)

END()
