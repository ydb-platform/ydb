LIBRARY()

SRCS(
    ext_tx_base.cpp
    write_queue.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/tracing
    library/cpp/lwtrace
)

END()
