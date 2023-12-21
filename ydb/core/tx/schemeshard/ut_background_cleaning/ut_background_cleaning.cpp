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

THolder<NConsole::TEvConsole::TEvConfigNotificationRequest> GetTestCompactionConfig(bool withRetries = false) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    auto* backgroundCleaningConfig = request->Record.MutableConfig()->MutableBackgroundCleaningConfig();
    backgroundCleaningConfig->SetMaxRate(1);
    backgroundCleaningConfig->SetMinWakeupIntervalMs(5);
    backgroundCleaningConfig->SetInflightLimit(1);

    auto* retrySettings = backgroundCleaningConfig->MutableRetrySettings();

    if (!withRetries) {
        retrySettings->SetMaxRetryNumber(1);
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
    auto request = GetTestCompactionConfig(withRetries);
    *request->Record.MutableConfig()->MutableFeatureFlags() = features;
    SetConfig(runtime, schemeShard, std::move(request));
}

void SetBackgroundCleaning(TTestActorRuntime &runtime, TTestEnv& env, ui64 schemeShard, bool withRetries = false) {
    NKikimrConfig::TFeatureFlags features;
    features.SetEnableTempTables(true);
    SetFeatures(runtime, env, schemeShard, features, withRetries);
}

void AsyncCreateTempTable(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& sessionActorId, ui32 nodeIdx) {
    auto ev = CreateTableRequest(txId, workingDir, scheme);
    auto* tx = ev->Record.MutableTransaction(0);
    auto* desc = tx->MutableCreateTable();
    desc->SetTemporary(true);
    desc->SetSessionActorId(sessionActorId.ToString());

    runtime.Send(new IEventHandle(MakeTabletResolverID(), sessionActorId,
            new TEvTabletResolver::TEvForward(schemeShardId, new IEventHandle(TActorId(), sessionActorId, ev), { },
                TEvTabletResolver::TEvForward::EActor::Tablet)), nodeIdx);
}

void TestCreateTempTable(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& scheme, const TActorId& sessionActorId, const TVector<TExpectedResult>& expectedResults, ui32 sessionNodeIdx) {
    AsyncCreateTempTable(runtime, TTestTxConfig::SchemeShard, txId, workingDir, scheme, sessionActorId, sessionNodeIdx);
    TestModificationResults(runtime, txId, expectedResults);
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
    auto description = DescribePrivatePath(runtime, schemeshardId,  fullPath, true, true);
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

} // namespace

Y_UNIT_TEST_SUITE(TSchemeshardBackgroundCleaningTest) {
    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestSimple) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto sessionActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestCreateTempTable(runtime, txId, "/MyRoot", R"(
                  Name: "TempTable"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId, { NKikimrScheme::StatusAccepted }, 1);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/TempTable");

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/TempTable", TTestTxConfig::SchemeShard, false);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestWithRetry) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard, true);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto sessionActorId = runtime.AllocateEdgeActor(1);

        ui64 txId = 100;
        TestCreateTempTable(runtime, txId, "/MyRoot", R"(
                  Name: "TempTable"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId, { NKikimrScheme::StatusAccepted }, 1);

        env.TestWaitNotification(runtime, txId);

        CheckTable(runtime, "/MyRoot/TempTable");

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);

        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));

        CheckTable(runtime, "/MyRoot/TempTable", TTestTxConfig::SchemeShard, true);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestTwoTables) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto sessionActorId1 = runtime.AllocateEdgeActor(1);
        ui64 txId1 = 100;
        TestCreateTempTable(runtime, txId1, "/MyRoot", R"(
                  Name: "TempTable1"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId1, { NKikimrScheme::StatusAccepted }, 1);
        env.TestWaitNotification(runtime, txId1);

        auto sessionActorId2 = runtime.AllocateEdgeActor(2);
        ui64 txId2 = ++txId1;
        TestCreateTempTable(runtime, txId2, "/MyRoot", R"(
                  Name: "TempTable2"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId2, { NKikimrScheme::StatusAccepted }, 2);
        env.TestWaitNotification(runtime, txId2);

        CheckTable(runtime, "/MyRoot/TempTable1");
        CheckTable(runtime, "/MyRoot/TempTable2");

        const TActorId proxy = runtime.GetInterconnectProxy(1, 0);
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0), 1, true);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        runtime.DispatchEvents(options);

        env.SimulateSleep(runtime, TDuration::Seconds(50));
        CheckTable(runtime, "/MyRoot/TempTable1", TTestTxConfig::SchemeShard, false);
        CheckTable(runtime, "/MyRoot/TempTable2", TTestTxConfig::SchemeShard);
    }

    Y_UNIT_TEST(SchemeshardBackgroundCleaningTestReboot) {
        TTestBasicRuntime runtime(3);
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        SetBackgroundCleaning(runtime, env, TTestTxConfig::SchemeShard);
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        auto sessionActorId1 = runtime.AllocateEdgeActor(1);
        ui64 txId1 = 100;
        TestCreateTempTable(runtime, txId1, "/MyRoot", R"(
                  Name: "TempTable1"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId1, { NKikimrScheme::StatusAccepted }, 1);
        env.TestWaitNotification(runtime, txId1);

        auto sessionActorId2 = runtime.AllocateEdgeActor(2);
        ui64 txId2 = ++txId1;
        TestCreateTempTable(runtime, txId2, "/MyRoot", R"(
                  Name: "TempTable2"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )", sessionActorId2, { NKikimrScheme::StatusAccepted }, 2);
        env.TestWaitNotification(runtime, txId2);

        CheckTable(runtime, "/MyRoot/TempTable1");
        CheckTable(runtime, "/MyRoot/TempTable2");

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
        CheckTable(runtime, "/MyRoot/TempTable1", TTestTxConfig::SchemeShard, false);
        CheckTable(runtime, "/MyRoot/TempTable2", TTestTxConfig::SchemeShard);
    }
};
