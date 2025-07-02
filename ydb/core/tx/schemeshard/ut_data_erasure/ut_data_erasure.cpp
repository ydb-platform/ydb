#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/data_erasure_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/storage_helpers.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {
    TTestEnv SetupEnv(TTestBasicRuntime& runtime, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies) {
        TTestEnv env(runtime, TTestEnvOptions()
            .NChannels(4)
            .EnablePipeRetries(true)
            .EnableSystemViews(false)
            .DSProxies(dsProxies));

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(50);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(10);

        return env;
    }

    void FillData(TTestBasicRuntime& runtime, ui64 schemeshardId, ui64& txId, const TVector<ui64>& shardsIds, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies, const TString& valueToDelete) {
        TString value(size_t(100 * 1024), 't');
        ui32 keyToDelete = 42;

        for (ui32 key : xrange(100)) {
            int partitionIdx = shardsIds.size() == 1 || key < 50 ? 0 : 1;
            WriteRow(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", partitionIdx, key, key == keyToDelete ? valueToDelete : value);
        }

        auto tableVersion = TestDescribeResult(DescribePath(runtime, schemeshardId, "/MyRoot/Database1/Simple"), {NLs::PathExist});
        for (const auto& shardsId : shardsIds) {
            const auto result = CompactTable(runtime, shardsId, tableVersion.PathId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        }

        DeleteRow(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", 0, keyToDelete);

        // BlobStorage should contain deleted value yet
        UNIT_ASSERT(BlobStorageContains(dsProxies, valueToDelete));
    }

    void CheckDataErasureStatus(TTestBasicRuntime& runtime, TActorId sender, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies, const TString& valueToDelete, bool completed) {
        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        if (completed) {
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
            UNIT_ASSERT(!BlobStorageContains(dsProxies, valueToDelete));
        } else {
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::IN_PROGRESS_TENANT);
            UNIT_ASSERT(BlobStorageContains(dsProxies, valueToDelete));
        }
    }
}

Y_UNIT_TEST_SUITE(TestDataErasure) {
    void SimpleDataErasureTest(const TSchemeObject& createSchemeObject, ui64 currentBscGeneration = 0) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(3);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        // Change BSC counter value between data erasure iterations
        if (currentBscGeneration > 1) {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(currentBscGeneration);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1", createSchemeObject);
        CreateTestExtSubdomain(runtime, env, &txId, "Database2", createSchemeObject);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, (currentBscGeneration >  1 ? 4 : 3)));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        if (currentBscGeneration > 1) {
            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), currentBscGeneration + 1, response->Record.GetGeneration());
        } else {
            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        }
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(SimpleTestForTables) {
        SimpleDataErasureTest({.Table = true, .Topic = false});
    }

    Y_UNIT_TEST(SimpleTestForTopic) {
        SimpleDataErasureTest({.Table = false, .Topic = true});
    }

    Y_UNIT_TEST(SimpleTestForAllSupportedObjects) {
        SimpleDataErasureTest({.Table = true, .Topic = true});
    }

    Y_UNIT_TEST(SchemeShardCounterDoesNotConsistWithBscCounter) {
        SimpleDataErasureTest({.Table = true, .Topic = false}, /*currentBscGeneration*/ 47);
    }

    void DataErasureRun3Cycles(const TSchemeObject& createSchemeObject) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(3);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1", createSchemeObject);
        CreateTestExtSubdomain(runtime, env, &txId, "Database2", createSchemeObject);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 9));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 3, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(Run3CyclesForTables) {
        DataErasureRun3Cycles({.Table = true, .Topic = false});
    }

    Y_UNIT_TEST(Run3CyclesForTopics) {
        DataErasureRun3Cycles({.Table = false, .Topic = true});
    }

    Y_UNIT_TEST(Run3CyclesForAllSupportedObjects) {
        DataErasureRun3Cycles({.Table = true, .Topic = true});
    }

    Y_UNIT_TEST(DataErasureManualLaunch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        {
            auto request = MakeHolder<TEvSchemeShard::TEvDataErasureManualStartupRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(ManualLaunch3Cycles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        auto RunDataErasure = [&runtime] (ui32 expectedGeneration) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvDataErasureManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
        };

        RunDataErasure(1);
        RunDataErasure(2);
        RunDataErasure(3);
    }

    Y_UNIT_TEST(ManualLaunch3CyclesWithNotConsistentCountersInSchemeShardAndBSC) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        auto RunDataErasure = [&runtime] (ui32 expectedGeneration, ui32 requiredCountShredResponses) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvDataErasureManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, requiredCountShredResponses));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
        };

        RunDataErasure(1, 3);
        // Change BSC counter value between data erasure iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(50);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        RunDataErasure(51, 4);
        // Change BSC counter value between data erasure iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(100);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        RunDataErasure(101, 4);
    }

    Y_UNIT_TEST(DataErasureWithCopyTable) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true, .Topic = false});
        auto shards = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        TString value(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards, dsProxies, value);

        // catch and hold borrow returns
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        TestCopyTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1", "SimpleCopy", "/MyRoot/Database1/Simple");

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // data cleanup should not be finished due to holded borrow returns
        CheckDataErasureStatus(runtime, sender, dsProxies, value, false);

        // return borrow
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // data cleanup should be finished after returned borrows
        CheckDataErasureStatus(runtime, sender, dsProxies, value, true);
    }

    Y_UNIT_TEST(DataErasureWithSplit) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = false, .Topic = false});

        TestCreateTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1",
            R"____(
                Name: "Simple"
                Columns { Name: "key1"  Type: "Uint32"}
                Columns { Name: "Value" Type: "Utf8"}
                KeyColumnNames: ["key1"]
            )____");
        env.TestWaitNotification(runtime, txId, schemeshardId);
        auto shards1 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1);

        TString valueToDelete(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards1, dsProxies, valueToDelete);

        // block borrow returns to suspend SplitTable
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        // block CollectGarbage requests to suspend DataCleanup
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime);
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        TestSplitTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", Sprintf(
            R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                    }
                }
            )", shards1.at(0)));
        env.TestWaitNotification(runtime, txId, schemeshardId);

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // DataErasure should be in progress because of SplitTable and DataCleanup have been suspended
        CheckDataErasureStatus(runtime, sender, dsProxies, valueToDelete, false);

        auto shards2 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 2);

        collectGarbageReqs.Stop().Unblock();
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // now data cleanup should be finished
        CheckDataErasureStatus(runtime, sender, dsProxies, valueToDelete, true);
    }

    Y_UNIT_TEST(DataErasureWithMerge) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = false, .Topic = false});

        TestCreateTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1",
            R"____(
                Name: "Simple"
                Columns { Name: "key1"  Type: "Uint32"}
                Columns { Name: "Value" Type: "Utf8"}
                KeyColumnNames: ["key1"]
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                    }
                }
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        MaxPartitionsCount: 2
                    }
                }
            )____");
        env.TestWaitNotification(runtime, txId, schemeshardId);
        auto shards1 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 2);

        TString valueToDelete(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards1, dsProxies, valueToDelete);

        // block borrow returns to suspend SplitTable
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        // block CollectGarbage requests to suspend DataCleanup
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime);
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        TestSplitTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", Sprintf(
            R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", shards1.at(0), shards1.at(1)));
        env.TestWaitNotification(runtime, txId, schemeshardId);

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // DataErasure should be in progress because of SplitTable and DataCleanup have been suspended
        CheckDataErasureStatus(runtime, sender, dsProxies, valueToDelete, false);

        auto shards2 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1);

        collectGarbageReqs.Stop().Unblock();
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // now data cleanup should be finished
        CheckDataErasureStatus(runtime, sender, dsProxies, valueToDelete, true);
    }
}
