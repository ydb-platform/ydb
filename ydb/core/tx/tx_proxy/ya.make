LIBRARY()

SRCS(
    mon.cpp
    proxy_impl.cpp
    schemereq.cpp
    datareq.cpp
    describe.cpp
    proxy.cpp
    read_table_impl.cpp
    resolvereq.cpp
    snapshotreq.cpp
    commitreq.cpp
    upload_rows_common_impl.cpp
    upload_rows.cpp
)

GENERATE_ENUM_SERIALIZATION(read_table_impl.h)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/actors/interconnect
    util/draft
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/docapi
    ydb/core/engine
    ydb/core/formats
    ydb/core/grpc_services
    ydb/core/io_formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/sys_view/common
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/tx/balance_coverage
    ydb/core/tx/datashard
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_allocator
    ydb/core/tx/tx_allocator_client
    ydb/library/aclib
    ydb/library/mkql_proto/protos
    ydb/public/lib/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_base_tenant
    ut_encrypted_storage
    ut_ext_tenant
    ut_storage_tenant
)
