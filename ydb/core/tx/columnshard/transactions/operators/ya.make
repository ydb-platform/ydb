LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL long_tx_write.cpp
    GLOBAL ev_write.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/transactions
)

END()
