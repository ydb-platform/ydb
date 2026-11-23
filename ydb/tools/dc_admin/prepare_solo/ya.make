PROGRAM(dc-admin-prepare-solo)

SRCS(main.cpp)

PEERDIR(
    library/cpp/streams/zstd
    ydb/apps/version
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/blobstorage/incrhuge
    ydb/core/blobstorage/nodewarden
    ydb/core/config/validation
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/tx/scheme_board
    ydb/library/keys
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

END()
