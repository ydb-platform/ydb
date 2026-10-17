YQL_LIBRARY()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    mon.cpp  # gn: public
    proxy_impl.cpp
    schemereq.cpp
    datareq.cpp
    describe.cpp
    long_tx_write_flow_control.cpp  # gn: upload_rows
    proxy.cpp  # gn: public
    read_table_impl.cpp  # gn: read_table
    resolvereq.cpp
    rpc_long_tx.cpp  # gn: upload_rows
    snapshotreq.cpp
    commitreq.cpp
    upload_columns.cpp
    upload_rows_counters.cpp  # gn: upload_rows
    upload_rows_common_impl.cpp  # gn: upload_rows
    upload_rows.cpp  # gn: upload_rows
)
# gn: public headers mon.h proxy.h
# gn: read_table headers read_table_impl.h
# gn: upload_rows headers long_tx_write_flow_control.h upload_rows.h upload_rows_common_impl.h upload_rows_counters.h

GENERATE_ENUM_SERIALIZATION(read_table_impl.h)
GENERATE_ENUM_SERIALIZATION(upload_rows_counters.h)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/helpers
    ydb/library/actors/interconnect
    util/draft
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/docapi
    ydb/core/engine
    ydb/core/formats
    ydb/core/grpc_services/local_rpc
    ydb/core/io_formats/arrow/scheme
    ydb/core/protos
    ydb/core/scheme
    ydb/core/security/sasl
    ydb/core/sys_view/common
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/tx/balance_coverage
    ydb/core/tx/columnshard/flow_control_manager
    ydb/core/tx/data_events
    ydb/core/tx/datashard
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_allocator
    ydb/core/tx/tx_allocator_client
    ydb/library/aclib
    ydb/library/login
    ydb/library/mkql_proto/protos
    ydb/public/lib/base
)
# gn: read_table peerdir ydb/core/tx/tx_proxy:public
# gn: upload_rows peerdir ydb/core/tx/tx_proxy:public

END()

RECURSE_FOR_TESTS(
    ut_base_tenant
    ut_encrypted_storage
    ut_ext_tenant
    ut_schemereq
    ut_storage_tenant
)
