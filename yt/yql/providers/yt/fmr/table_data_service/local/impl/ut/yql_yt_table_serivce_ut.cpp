#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>

using namespace NYql::NFmr;

TString key = "table_id_part_id:0";

Y_UNIT_TEST_SUITE(TLocalTableServiceTest)
{
    Y_UNIT_TEST(GetNonexistentKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        auto getFuture = tableDataService->Get(key);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetExistingKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        tableDataService->Put(key, "1").GetValueSync();
        auto getFuture = tableDataService->Get(key);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteNonexistentKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        tableDataService->Put(key, "1").GetValueSync();
        tableDataService->Delete("key:2").GetValueSync();
        auto getFuture = tableDataService->Get(key);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteExistingKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        tableDataService->Put(key, "1").GetValueSync();
        // deleting by prefix
        auto deleteFuture = tableDataService->RegisterDeletion({"table_id_part_id", "other_group"});
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService->Get(key);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetStatistics) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        tableDataService->Put(key, "12345").GetValueSync();
        tableDataService->Put("other_group:4", "678").GetValueSync();
        auto stats = tableDataService->GetStatistics().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(stats.KeysNum, 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataWeight, 8);
    }
}
