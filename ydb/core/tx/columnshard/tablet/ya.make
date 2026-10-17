LIBRARY()

SRCS(
    ext_tx_base.cpp  # gn: into ydb/core/tx/columnshard
    write_queue.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/tracing
    library/cpp/lwtrace
)

END()
