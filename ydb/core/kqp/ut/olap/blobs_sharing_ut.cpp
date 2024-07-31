#include "helpers/typed_local.h"
#include "helpers/local.h"
#include "helpers/writer.h"
#include <ydb/core/tx/columnshard/data_sharing/initiator/controller/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/data_sharing/initiator/status/abstract.h>
#include <ydb/core/tx/columnshard/data_sharing/common/context/context.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/control.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_ss_tasks/task.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapBlobsSharing) {

    namespace {
    class TTransferStatus {
    private:
        YDB_ACCESSOR(bool, Proposed, false);
        YDB_ACCESSOR(bool, Confirmed, false);
        YDB_ACCESSOR(bool, Finished, false);
    public:
        void Reset() {
            Confirmed = false;
            Proposed = false;
            Finished = false;
        }
    };

    static TMutex CSTransferStatusesMutex;
    static std::shared_ptr<TTransferStatus> CSTransferStatus = std::make_shared<TTransferStatus>();
    }

    class TTestController: public NOlap::NDataSharing::IInitiatorController {
    private:
        static const inline auto Registrator = TFactory::TRegistrator<TTestController>("test");
    protected:
        virtual void DoProposeError(const TString& sessionId, const TString& message) const override {
            AFL_VERIFY(false)("session_id", sessionId)("message", message);
        }
        virtual void DoProposeSuccess(const TString& sessionId) const override {
            CSTransferStatus->SetProposed(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_proposed")("session_id", sessionId);
        }
        virtual void DoConfirmSuccess(const TString& sessionId) const override {
            CSTransferStatus->SetConfirmed(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_confirmed")("session_id", sessionId);
        }
        virtual void DoFinished(const TString& sessionId) const override {
            CSTransferStatus->SetFinished(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_finished")("session_id", sessionId);
        }
        virtual void DoStatus(const NOlap::NDataSharing::TStatusContainer& status) const override {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "status")("info", status.SerializeToProto().DebugString());
        }
        virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) override {
            return TConclusionStatus::Success();
        }
        virtual void DoSerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) const override {

        }

        virtual TString GetClassName() const override {
            return "test";
        }
    };

    class TSharingDataTestCase {
    private:
        const ui32 ShardsCount;
        TKikimrRunner& Kikimr;
        TTypedLocalHelper Helper;
        NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller;
        std::vector<ui64> ShardIds;
        std::vector<ui64> PathIds;
        YDB_ACCESSOR(bool, RebootTablet, false);
    public:
        const TTypedLocalHelper& GetHelper() const {
            return Helper;
        }

        void AddRecords(const ui32 recordsCount, const double kff = 0) {
            Helper.FillPKOnly(kff, recordsCount);
        }

        TSharingDataTestCase(const ui32 shardsCount, TKikimrRunner& kikimr)
            : ShardsCount(shardsCount)
            , Kikimr(kikimr)
            , Helper("", Kikimr, "olapTable", "olapStore12")
            , Controller(NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>()) {
            Controller->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
            Controller->SetExpectedShardsCount(ShardsCount);
            Controller->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            Controller->SetReadTimeoutClean(TDuration::Seconds(1));

            Tests::NCommon::TLoggerInit(Kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS").Initialize();

            Helper.CreateTestOlapTable(ShardsCount, ShardsCount);
            ShardIds = Controller->GetShardActualIds();
            AFL_VERIFY(ShardIds.size() == ShardsCount)("count", ShardIds.size())("ids", JoinSeq(",", ShardIds));
            std::set<ui64> pathIdsSet;
            for (auto&& i : ShardIds) {
                auto pathIds = Controller->GetPathIds(i);
                pathIdsSet.insert(pathIds.begin(), pathIds.end());
            }
            PathIds = std::vector<ui64>(pathIdsSet.begin(), pathIdsSet.end());
            AFL_VERIFY(PathIds.size() == 1)("count", PathIds.size())("ids", JoinSeq(",", PathIds));
        }

        void WaitNormalization() {
            Controller->SetReadTimeoutClean(TDuration::Seconds(1));
            Controller->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);
            const auto start = TInstant::Now();
            while (!Controller->IsTrivialLinks() && TInstant::Now() - start < TDuration::Seconds(30)) {
                Cerr << "WAIT_TRIVIAL_LINKS..." << Endl;
                Sleep(TDuration::Seconds(1));
            }
            AFL_VERIFY(Controller->IsTrivialLinks());
            Controller->CheckInvariants();
            Controller->SetReadTimeoutClean(TDuration::Minutes(5));
        }

        void Execute(const ui64 destinationIdx, const std::vector<ui64>& sourceIdxs, const bool move, const NOlap::TSnapshot& snapshot, const std::set<ui64>& pathIdxs) {
            Controller->SetReadTimeoutClean(TDuration::Seconds(1));
            AFL_VERIFY(destinationIdx < ShardIds.size());
            const ui64 destination = ShardIds[destinationIdx];
            std::vector<ui64> sources;
            for (auto&& i : sourceIdxs) {
                AFL_VERIFY(i < ShardIds.size());
                sources.emplace_back(ShardIds[i]);
            }
            std::set<ui64> pathIds;
            for (auto&& i : pathIdxs) {
                AFL_VERIFY(i < PathIds.size());
                AFL_VERIFY(pathIds.emplace(PathIds[i]).second);
            }
            Cerr << "SHARING: " << JoinSeq(",", sources) << "->" << destination << Endl;
            THashMap<ui64, ui64> pathIdsRemap;
            for (auto&& i : pathIds) {
                pathIdsRemap.emplace(i, i);
            }
            THashSet<NOlap::TTabletId> sourceTablets;
            for (auto&& i : sources) {
                AFL_VERIFY(sourceTablets.emplace((NOlap::TTabletId)i).second);
            }
            const TString sessionId = TGUID::CreateTimebased().AsUuidString();
            NOlap::NDataSharing::TTransferContext transferContext((NOlap::TTabletId)destination, sourceTablets, snapshot, move);
            NOlap::NDataSharing::TDestinationSession session(std::make_shared<TTestController>(), pathIdsRemap, sessionId, transferContext);
            Kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                new NOlap::NDataSharing::NEvents::TEvProposeFromInitiator(session), destination, false));
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetProposed() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_PROPOSING..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetProposed());
            }
            if (RebootTablet) {
                Kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                    new TEvents::TEvPoisonPill(), destination, false));
            }
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetConfirmed() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new NOlap::NDataSharing::NEvents::TEvConfirmFromInitiator(sessionId), destination, false));
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_CONFIRMED..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetConfirmed());
            }
            if (RebootTablet) {
                Kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                    new TEvents::TEvPoisonPill(), destination, false));
                for (auto&& i : sources) {
                    Kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
            }
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetFinished() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_FINISHED..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetFinished());
            }
            CSTransferStatus->Reset();
            AFL_VERIFY(!Controller->IsTrivialLinks());
            Controller->CheckInvariants();
            Controller->SetReadTimeoutClean(TDuration::Minutes(5));
        }
    };
    Y_UNIT_TEST(BlobsSharingSplit1_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, { 1 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
    }

    Y_UNIT_TEST(BlobsSharingSplit1_1_clean) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(2, kikimr);
        tester.AddRecords(80000);
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[80000u;]])");
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, { 1 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[119928u;]])");
        tester.AddRecords(80000, 0.8);
        tester.WaitNormalization();
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[183928u;]])");
    }

    Y_UNIT_TEST(BlobsSharingSplit1_1_clean_with_restarts) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(2, kikimr);
        tester.SetRebootTablet(true);
        tester.AddRecords(80000);
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[80000u;]])");
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, { 1 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[119928u;]])");
        tester.AddRecords(80000, 0.8);
        tester.WaitNormalization();
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[183928u;]])");
    }

    Y_UNIT_TEST(BlobsSharingSplit3_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, { 1, 2, 3 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
    }

    Y_UNIT_TEST(BlobsSharingSplit1_3_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(1, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(2, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(3, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(0, { 1, 2, 3 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
    }

    Y_UNIT_TEST(BlobsSharingSplit1_3_2_1_clean) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(1, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(2, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(3, { 0 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.AddRecords(800000, 0.9);
        Sleep(TDuration::Seconds(1));
        tester.Execute(3, { 2 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.Execute(0, { 1, 2 }, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), { 0 });
        tester.WaitNormalization();
    }

    class TReshardingTest {
    private:
        YDB_ACCESSOR(TString, ShardingType, "HASH_FUNCTION_CONSISTENCY_64");

        void WaitResharding(const TString& hint = "") {
            const TInstant start = TInstant::Now();
            bool clean = false;
            while (TInstant::Now() - start < TDuration::Seconds(20)) {
                NYdb::NOperation::TOperationClient operationClient(Kikimr.GetDriver());
                auto result = operationClient.List<NYdb::NSchemeShard::TBackgroundProcessesResponse>().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                if (result.GetList().size() == 0) {
                    Cerr << "RESHARDING_FINISHED" << Endl;
                    clean = true;
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
                Sleep(TDuration::Seconds(1));
                Cerr << "RESHARDING_WAIT_FINISHED... (" << hint << ")" << Endl;
            }
            AFL_VERIFY(clean);
        }

        void CheckCount(const ui32 expectation) {
            auto it = Kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << result << Endl;
            CompareYson(result, "[[" + ::ToString(expectation) + "u;]]");
        }

        TKikimrRunner Kikimr;
    public:

        TReshardingTest()
            : Kikimr(TKikimrSettings().SetWithSampleTables(false)) {

        }

        void Execute() {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
            csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            csController->SetLagForCompactionBeforeTierings(TDuration::Seconds(1));
            csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

            TLocalHelper(Kikimr).SetShardingMethod(ShardingType).CreateTestOlapTable("olapTable", "olapStore", 24, 4);
            auto tableClient = Kikimr.GetTableClient();

            Tests::NCommon::TLoggerInit(Kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD, NKikimrServices::TX_COLUMNSHARD_SCAN }, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

            {
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
                WriteTestData(Kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);
            }

            CheckCount(230000);
            for (ui32 i = 0; i < 2; ++i) {
                auto alterQuery = TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore/olapTable` (TYPE TABLESTORE) SET (ACTION=ALTER_SHARDING, MODIFICATION=SPLIT);)";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
                WaitResharding("SPLIT:" + ::ToString(i));
            }
            {
                auto alterQuery = TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore/olapTable` (TYPE TABLESTORE) SET (ACTION=ALTER_SHARDING, MODIFICATION=SPLIT);)";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            AFL_VERIFY(csController->GetShardingFiltersCount().Val() == 0);
            CheckCount(230000);
            i64 count = csController->GetShardingFiltersCount().Val();
            AFL_VERIFY(count >= 16)("count", count);
            csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            csController->WaitIndexation(TDuration::Seconds(3));
            csController->WaitCompactions(TDuration::Seconds(3));
            WriteTestData(Kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            CheckCount(230000);
            csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            csController->WaitIndexation(TDuration::Seconds(5));
            CheckCount(230000);
            csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            csController->WaitCompactions(TDuration::Seconds(5));
            count = csController->GetShardingFiltersCount().Val();
            CheckCount(230000);

            csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);

            CheckCount(230000);

            AFL_VERIFY(count == csController->GetShardingFiltersCount().Val())("count", count)("val", csController->GetShardingFiltersCount().Val());
            const ui32 portionsCount = 16;
            for (ui32 i = 0; i < 4; ++i) {
                {
                    auto alterQuery = TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore/olapTable` (TYPE TABLESTORE) SET (ACTION=ALTER_SHARDING, MODIFICATION=MERGE);)";
                    auto session = tableClient.CreateSession().GetValueSync().GetSession();
                    auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
                }
                WaitResharding("MERGE:" + ::ToString(i));
                //            csController->WaitCleaning(TDuration::Seconds(5));

                CheckCount(230000);
                AFL_VERIFY(count + portionsCount == csController->GetShardingFiltersCount().Val())("count", count)("val", csController->GetShardingFiltersCount().Val());
                count += portionsCount;
            }
            {
                auto alterQuery = TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore/olapTable` (TYPE TABLESTORE) SET (ACTION=ALTER_SHARDING, MODIFICATION=MERGE);)";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            csController->CheckInvariants();
        }
    };

    Y_UNIT_TEST(TableReshardingConsistency64) {
        TReshardingTest().SetShardingType("HASH_FUNCTION_CONSISTENCY_64").Execute();
    }

    Y_UNIT_TEST(TableReshardingModuloN) {
        TReshardingTest().SetShardingType("HASH_FUNCTION_MODULO_N").Execute();
    }
}
}
