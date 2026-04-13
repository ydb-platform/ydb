#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>

using namespace NYql::NFmr;

const TString group = "table_id_part_id:";
const TString chunkId = "0";

Y_UNIT_TEST_SUITE(TLocalTableServiceTest)
{
    Y_UNIT_TEST(GetNonexistentKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        auto getFuture = tableDataService->Get(group, chunkId);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetExistingKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        UNIT_ASSERT(tableDataService->Put(group, chunkId, "1").GetValueSync());
        auto getFuture = tableDataService->Get(group, chunkId);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteNonexistentKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        UNIT_ASSERT(tableDataService->Put(group, chunkId,  "1").GetValueSync());
        tableDataService->Delete(group, "2").GetValueSync();
        auto getFuture = tableDataService->Get(group, chunkId);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteExistingKey) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        UNIT_ASSERT(tableDataService->Put(group, chunkId,"1").GetValueSync());
        // deleting by prefix
        auto deleteFuture = tableDataService->RegisterDeletion({group, "other_group"});
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService->Get(group, chunkId);
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetStatistics) {
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService();
        UNIT_ASSERT(tableDataService->Put(group, chunkId, "12345").GetValueSync());
        UNIT_ASSERT(tableDataService->Put("other_group", "other_chunk_id", "678").GetValueSync());
        auto stats = tableDataService->GetStatistics().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(stats.KeysNum, 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataWeight, 8);
    }
    Y_UNIT_TEST(MaxDataWeightExceeded) {
        ui64 maxDataWeight = 10;
        ILocalTableDataService::TPtr tableDataService = MakeLocalTableDataService(TTableDataServiceSettings{maxDataWeight});
        UNIT_ASSERT(tableDataService->Put(group, chunkId, "12345").GetValueSync());
        UNIT_ASSERT(!tableDataService->Put(group, "other_chunk_id", "678910111213").GetValueSync());
    }
}
