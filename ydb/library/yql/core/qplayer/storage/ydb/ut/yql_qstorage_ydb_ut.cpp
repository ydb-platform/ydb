#include <ydb/library/yql/core/qplayer/storage/ydb/yql_qstorage_ydb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/core/qplayer/storage/ut_common/yql_qstorage_ut_common.h>

#include <util/generic/guid.h>
#include <util/system/env.h>

using namespace NYql;

IQStoragePtr MakeTestYdbQStorage() {
    TYdbQStorageSettings settings;
    settings.Token = GetEnv("YDB_TOKEN");
    settings.Endpoint = GetEnv("YDB_QSTORAGE_ENDPOINT");
    if (!settings.Endpoint) {
        Cerr << "YDB_QSTORAGE_ENDPOINT is not set";
        return nullptr;
    }

    settings.Database = GetEnv("YDB_QSTORAGE_DATABASE");
    settings.TablesPrefix = GetEnv("YDB_QSTORAGE_TABLES_PREFIX");
    settings.OperationIdPrefix = CreateGuidAsString() + "_";
    settings.PartBytes = 2;
    settings.MaxBatchSize = 30;
    return MakeYdbQStorage(settings);
}

Y_UNIT_TEST_SUITE(TQStorageFileTests) {
    GENERATE_TESTS(MakeTestYdbQStorage)
}
