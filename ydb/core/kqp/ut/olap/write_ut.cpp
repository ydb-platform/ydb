#include "helpers/local.h"
#include "helpers/writer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapWrite) {
    Y_UNIT_TEST(TierDraftsGC) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetInsertStartedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() || !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        const auto startInstant = TMonotonic::Now();
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
            Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
            Sleep(TDuration::Seconds(2));
        }

        AFL_VERIFY(!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
    }

    Y_UNIT_TEST(TierDraftsGCWithRestart) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1000));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetInsertStartedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() < 20 || !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        {
            const auto startInstant = TMonotonic::Now();
            AFL_VERIFY(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() == 0)("count", Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount());
            while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
                for (auto&& i : csController->GetShardActualIds()) {
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
                csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
                Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
        }

        {
            const auto startInstant = TMonotonic::Now();
            while (TMonotonic::Now() - startInstant < TDuration::Seconds(10)) {
                for (auto&& i : csController->GetShardActualIds()) {
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
                Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() << Endl;
                Sleep(TDuration::MilliSeconds(500));
            }
        }

        AFL_VERIFY(!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        const auto writesCount = Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount();
        const auto deletesCount = Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount();
        AFL_VERIFY(deletesCount <= writesCount + 1)("writes", writesCount)("deletes", deletesCount);
    }

}

} // namespace
