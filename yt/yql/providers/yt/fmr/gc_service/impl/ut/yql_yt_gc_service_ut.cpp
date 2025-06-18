#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(GcServiceTests) {
    Y_UNIT_TEST(GroupDeletionRequest) {
        TString group = "table_id_part_id";
        TString content = "test_content";
        ui64 keysNum = 10000;
        auto tableDataService = MakeLocalTableDataService();
        auto gcService = MakeGcService(tableDataService);

        for (ui64 i = 0; i < keysNum; ++i) {
            TString key = group + ":" + ToString(i);
            tableDataService->Put(key, content +  ToString(i)).GetValueSync();
        }
        gcService->ClearGarbage({group}).GetValueSync();
        Sleep(TDuration::Seconds(3)); // deleting by prefix, after reigster wait some time for actual deletion

        for (ui64 i = 0; i < keysNum; ++i) {
            TString key = group + ":" + ToString(i);
            UNIT_ASSERT(!tableDataService->Get(key).GetValueSync());
        }
    }
    Y_UNIT_TEST(MaxInflightGroupDeletionRequestsExceeded) {
        TGcServiceSettings gcServiceSettings{};
        gcServiceSettings.GroupDeletionRequestMaxBatchSize = 5;
        gcServiceSettings.MaxInflightGroupDeletionRequests = 10;
        auto tableDataService = MakeLocalTableDataService();
        auto gcService = MakeGcService(tableDataService, gcServiceSettings);

        ui64 requestsNum = 50;
        std::vector<NThreading::TFuture<void>> clearGarbageFutures;
        for (ui64 i = 0; i < requestsNum; ++i) {
            std::vector<TString> groupsToDelete;
            for (int j = 0; j < 5; ++j) {
                groupsToDelete.emplace_back("group" + ToString(i) + "_" + ToString(j));
            }
            clearGarbageFutures.emplace_back(gcService->ClearGarbage(groupsToDelete));
        }
        auto totalDeletionFuture = NThreading::WaitExceptionOrAll(clearGarbageFutures);
        Sleep(TDuration::Seconds(10));
        UNIT_ASSERT(!totalDeletionFuture.HasValue());
    }
}

} // namespace NYql::NFmr
