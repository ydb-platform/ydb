#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>

using namespace NYql::NFmr;

Y_UNIT_TEST_SUITE(TLocalTableServiceTest)
{
    Y_UNIT_TEST(GetNonexistentKey) {
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService({3});
        auto getFuture = tableDataService->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetExistingKey) {
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService({3});
        auto putFuture = tableDataService->Put("key", "1");
        putFuture.GetValueSync();
        auto getFuture = tableDataService->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteNonexistentKey) {
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService({3});
        auto putFuture = tableDataService->Put("key", "1");
        putFuture.GetValueSync();
        auto deleteFuture = tableDataService->Delete("other_key");
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteExistingKey) {
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService({3});
        auto putFuture = tableDataService->Put("key", "1");
        putFuture.GetValueSync();
        auto deleteFuture = tableDataService->Delete("key");
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
}
