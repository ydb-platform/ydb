UNITTEST()

SRCS(
    endpoint_select_ut.cpp
    ../endpoint_select.cpp
)

PEERDIR(
    ydb/public/api/protos
)

SIZE(SMALL)

END()
