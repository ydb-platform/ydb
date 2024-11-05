#include <ydb/core/cms/console/console.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <algorithm>
#include <random>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

THolder<NConsole::TEvConsole::TEvConfigNotificationRequest> GetTestBackgroundCleaningConfig(bool withRetries = false) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    auto* backgroundCleaningConfig = request->Record.MutableConfig()->MutableBackgroundCleaningConfig();
    backgroundCleaningConfig->SetMaxRate(10);
    backgroundCleaningConfig->SetMinWakeupIntervalMs(500);
    backgroundCleaningConfig->SetInflightLimit(10);

    auto* retrySettings = backgroundCleaningConfig->MutableRetrySettings();

    if (!withRetries) {
        retrySettings->SetMaxRetryNumber(0);
    }

    return request;
}

void SetFeatures(
    TTestActorRuntime &runtime,
    TTestEnv&,
    ui64 schemeShard,
    const NKikimrConfig::TFeatureFlags& features,
    bool withRetries = false)
{
    auto request = GetTestBackgroundCleaningConfig(withRetries);
    *request->Record.MutableConfig()->MutableFeatureFlags() = features;
    SetConfig(runtime, schemeShard, std::move(request));
}

void SetBackgroundCleaning(TTestActorRuntime &runtime, TTestEnv& env, ui64 schemeShard, bool withRetries = false) {
    NKikimrConfig::TFeatureFlags features;
    features.SetEnableTempTables(true);
    SetFeatures(runtime, env, schemeShard, features, withRetries);
}

void AsyncCreateTable(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, ui32 nodeIdx, bool allowCreateInTempDir) {
    auto ev = CreateIndexedTableRequest(txId, workingDir, scheme);
    auto* tx = ev->Record.MutableTransaction(0);
    tx->SetAllowCreateInTempDir(allowCreateInTempDir);
    AsyncSend(runtime, schemeShardId, ev, nodeIdx, ownerActorId);
}

void AsyncMkDir(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, ui32 nodeIdx, bool allowCreateInTempDir) {
    auto ev = MkDirRequest(txId, workingDir, scheme);
    auto* tx = ev->Record.MutableTransaction(0);
    tx->SetAllowCreateInTempDir(allowCreateInTempDir);
    AsyncSend(runtime, schemeShardId, ev, nodeIdx, ownerActorId);
}

void AsyncMkTempDir(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, ui32 nodeIdx, bool allowCreateInTempDir) {
    auto ev = MkDirRequest(txId, workingDir, scheme);
    auto* tx = ev->Record.MutableTransaction(0);
    tx->SetAllowCreateInTempDir(allowCreateInTempDir);
    ActorIdToProto(ownerActorId, tx->MutableTempDirOwnerActorId());
    AsyncSend(runtime, schemeShardId, ev, nodeIdx, ownerActorId);
}

void TestMkDir(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, const TVector<TExpectedResult>& expectedResults, ui32 ownerNodeIdx, bool allowCreateInTempDir) {
    AsyncMkDir(runtime, TTestTxConfig::SchemeShard, txId, workingDir, scheme, ownerActorId, ownerNodeIdx, allowCreateInTempDir);
    TestModificationResults(runtime, txId, expectedResults);
}

void TestMkTempDir(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, const TVector<TExpectedResult>& expectedResults, ui32 ownerNodeIdx, bool allowCreateInTempDir) {
    AsyncMkTempDir(runtime, TTestTxConfig::SchemeShard, txId, workingDir, scheme, ownerActorId, ownerNodeIdx, allowCreateInTempDir);
    TestModificationResults(runtime, txId, expectedResults);
}

void TestCreateTable(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& ownerActorId, const TVector<TExpectedResult>& expectedResults, ui32 ownerNodeIdx, bool allowCreateInTempDir) {
    AsyncCreateTable(runtime, TTestTxConfig::SchemeShard, txId, workingDir, scheme, ownerActorId, ownerNodeIdx, allowCreateInTempDir);
    TestModificationResults(runtime, txId, expectedResults);
}

void TestDropTempTable(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir,
        const TString& scheme, bool checkUnsubscribe) {
    TestDropTable(runtime, ++txId, workingDir, scheme);
    if (checkUnsubscribe) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Unsubscribe);
        runtime.DispatchEvents(options);
    }
}

THashSet<TString> GetTables(
    TTestActorRuntime &runtime,
    ui64 tabletId)
{
    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvGetInfoRequest>();
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    THashSet<TString> result;

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
    for (auto& table: response->Record.GetUserTables()) {
        result.insert(table.GetName());
    }

    return result;
}

void CheckTable(
    TTestActorRuntime &runtime,
    const char* fullPath,
    ui64 schemeshardId = TTestTxConfig::SchemeShard,
    bool checkExists = true)
{
    TVector<ui64> shards;
    auto description = DescribePrivatePath(runtime, schemeshardId, fullPath, true, true);
    if (!checkExists) {
        UNIT_ASSERT(description.GetStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist);
        return;
    }
    for (auto &part : description.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    auto tables = GetTables(runtime, shards.at(0));
    auto userTableName = TStringBuf(fullPath).RNextTok('/');

    UNIT_ASSERT(tables.contains(userTableName));
}

void CheckPath(
    TTestActorRuntime &runtime,
    const char* fullPath,
    ui64 schemeshardId = TTestTxConfig::SchemeShard,
    bool checkExists = true) {
    auto description = DescribePrivatePath(runtime, schemeshardId, fullPath, true, true);
    if (checkExists) {
        UNIT_ASSERT(description.GetStatus() == NKikimrScheme::EStatus::StatusSuccess);
    } else {
        UNIT_ASSERT(description.GetStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeshardBackgroundCleaningTest) {
    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestSimpleCreateClean) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;

        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;
        TestMkDir(runtime, txId, "/MyRoot/tmp", "a", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;
        TestMkDir(runtime, txId, "/MyRoot/tmp/a", "b", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp/a/b", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/a/b/TempTable");
        CheckPath(runtime, "/MyRoot/tmp/a/b", TTestTxConfig::SchemeShard, true);
        CheckPath(runtime, "/MyRoot/tmp/a", TTestTxConfig::SchemeShard, true);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/tmp/a/b/TempTable", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp/a/b", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp/b", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, false);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestCreateCleanWithRetry) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard, true);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;

        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/TempTable");
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);

        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));

        CheckTable(runtime, "/MyRoot/tmp/TempTable", TTestTxConfig::SchemeShard, true);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestCreateCleanManyTables) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);
        ui64 txId = 100;

        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);

        ++txId;
        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable1"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);

        ++txId;
        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable2"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/TempTable1");
        CheckTable(runtime, "/MyRoot/tmp/TempTable2");
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvInterconnect::EvNodeConnected);
            runtime.DispatchEvents(options);
        }

        CheckTable(runtime, "/MyRoot/tmp/TempTable1", TTestTxConfig::SchemeShard, false);
        CheckTable(runtime, "/MyRoot/tmp/TempTable2", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, false);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestReboot) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId1 = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestMkTempDir(runtime, txId, "/MyRoot", "tmp1", ownerActorId1, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);

        ui64 txId1 = ++txId;
        TestCreateTable(runtime, txId1, "/MyRoot/tmp1", R"(
            TableDescription {
                Name: "TempTable1"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId1, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId1);

        auto ownerActorId2 = runtime.AllocateEdgeActor(2);

        ++txId;
        TestMkTempDir(runtime, txId, "/MyRoot", "tmp2", ownerActorId2, { NKikimrScheme::StatusAccepted }, 2, false);
        env.TestWaitNotification(runtime, txId);

        ui64 txId2 = ++txId;
        TestCreateTable(runtime, txId2, "/MyRoot/tmp2", R"(
            TableDescription {
                Name: "TempTable2"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId2, { NKikimrScheme::StatusAccepted }, 2, true);
        env.TestWaitNotification(runtime, txId2);

        CheckTable(runtime, "/MyRoot/tmp1/TempTable1");
        CheckTable(runtime, "/MyRoot/tmp2/TempTable2");

        CheckPath(runtime, "/MyRoot/tmp1", TTestTxConfig::SchemeShard, true);
        CheckPath(runtime, "/MyRoot/tmp1", TTestTxConfig::SchemeShard, true);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        env.SimulateSleep(runtime, TDuration::Seconds(30));
        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/tmp1/TempTable1", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp1", TTestTxConfig::SchemeShard, false);

        CheckTable(runtime, "/MyRoot/tmp2/TempTable2", TTestTxConfig::SchemeShard);
        CheckPath(runtime, "/MyRoot/tmp2", TTestTxConfig::SchemeShard, true);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestSimpleDrop) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/TempTable");
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        ++txId;
        TestDropTempTable(runtime, txId, "/MyRoot/tmp", "TempTable", true);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/tmp/TempTable", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestSimpleDropIndex) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/TempTable");

        TestLs(runtime, "/MyRoot/tmp/TempTable/ValueIndex", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        ++txId;
        TestDropTempTable(runtime, txId, "/MyRoot/tmp", "TempTable", true);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/tmp/TempTable", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        TestLs(runtime, "/MyRoot/tmp/TempTable/ValueIndex", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathNotExist);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestSimpleCleanIndex) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/tmp/TempTable");
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, true);

        TestLs(runtime, "/MyRoot/tmp/TempTable/ValueIndex", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));

        CheckTable(runtime, "/MyRoot/tmp/TempTable", TTestTxConfig::SchemeShard, false);
        CheckPath(runtime, "/MyRoot/tmp", TTestTxConfig::SchemeShard, false);

        TestLs(runtime, "/MyRoot/tmp/TempTable/ValueIndex", TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathNotExist);
    }

    Y_UNIT_TEST(TempInTemp) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;

        TestMkDir(runtime, txId, "/MyRoot", "test", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        // Can't create temp dir with flag
        TestMkTempDir(runtime, txId, "/MyRoot/test", "tmp", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;
        
        TestMkTempDir(runtime, txId, "/MyRoot/test", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        // Can't create temp dir inside temp dir
        TestMkTempDir(runtime, txId, "/MyRoot/test/tmp", "tmp2", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;
        TestMkTempDir(runtime, txId, "/MyRoot/test/tmp", "tmp2", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestMkDir(runtime, txId, "/MyRoot/test/tmp", "a", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestMkDir(runtime, txId, "/MyRoot/test/tmp/a", "b", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        // Can't create temp dir inside temp dir (with nested dirs)
        TestMkTempDir(runtime, txId, "/MyRoot/test/tmp/a/b", "tmp2", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;
        TestMkTempDir(runtime, txId, "/MyRoot/test/tmp/a/b", "tmp2", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;
    }

    Y_UNIT_TEST(CreateTableInTemp) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto ownerActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;

        TestMkDir(runtime, txId, "/MyRoot", "nottmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestMkTempDir(runtime, txId, "/MyRoot", "tmp", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/nottmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "TempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, true);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/nottmp", R"(
            TableDescription {
                Name: "NotTempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusAccepted }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;

        TestCreateTable(runtime, txId, "/MyRoot/tmp", R"(
            TableDescription {
                Name: "NotTempTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
            }
        )", ownerActorId, { NKikimrScheme::StatusPreconditionFailed }, 1, false);
        env.TestWaitNotification(runtime, txId);
        ++txId;
    }
};
