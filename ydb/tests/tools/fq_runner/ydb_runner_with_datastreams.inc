ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_ENABLE_DATASTREAMS=true)
ENV(YDB_PQ_CLIENT_SERVICE_TYPES=yandex-query)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner.inc)
