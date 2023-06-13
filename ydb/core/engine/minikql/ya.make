LIBRARY()

SRCS(
    flat_local_minikql_host.h
    flat_local_tx_factory.cpp
    minikql_engine_host.cpp
    minikql_engine_host.h
)

PEERDIR(
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/engine
    ydb/core/formats
    ydb/core/tablet_flat
    ydb/library/yql/parser/pg_wrapper/interface
)

YQL_LAST_ABI_VERSION()

END()
