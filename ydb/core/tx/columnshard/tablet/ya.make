LIBRARY()

SRCS(
    ext_tx_base.h
    write_queue.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
)

END()
