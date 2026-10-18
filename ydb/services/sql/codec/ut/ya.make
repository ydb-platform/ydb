UNITTEST()

SIZE(SMALL)

SRCS(
    ints_ut.cpp
    text_ut.cpp
    time_ut.cpp
)

PEERDIR(
    ydb/services/sql/codec
)

END()
