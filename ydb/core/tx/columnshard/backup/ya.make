LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/backup/async_jobs
    ydb/core/tx/columnshard/backup/iscan
    ydb/core/tx/columnshard/backup/import
)

END()

RECURSE_FOR_TESTS(
    async_jobs
    import
    iscan
)
