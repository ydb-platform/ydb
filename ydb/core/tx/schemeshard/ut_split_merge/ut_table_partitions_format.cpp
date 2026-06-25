#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers_flags_n.h>
#include <ydb/core/tx/schemeshard/ut_helpers/schemeshard_counters.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/http/http.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// Also used in ut_split_merge.cpp
TString PostSwitchAction(TTestActorRuntime& runtime, ui64 schemeShard, TPathId pathId, TStringBuf format) {
    auto sender = runtime.AllocateEdgeActor();
    const TString query = TStringBuilder()
    << "/app?Action=TablePartitionsFormatSwitch"
    << "&OwnerPathId=" << pathId.OwnerId
    << "&LocalPathId=" << pathId.LocalPathId
    << "&format=" << format
    ;
    auto req = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>(query, HTTP_METHOD_POST);
    runtime.SendToPipe(schemeShard, sender, req.release(), 0, {});
    auto r = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteBinaryInfoRes>(sender);
    return r->Get()->Blob;
}

TPathId GetPathId(TTestActorRuntime& runtime, const TString& path) {
    auto desc = DescribePath(runtime, path);
    const auto& self = desc.GetPathDescription().GetSelf();
    return TPathId(self.GetSchemeshardId(), self.GetPathId());
}

namespace {

TString PostSweepAction(TTestActorRuntime& runtime, ui64 schemeShard, const TString& params) {
    auto sender = runtime.AllocateEdgeActor();
    TString redirectLocation;
    {
        const TString query = TStringBuilder() << "/app?TabletID=" << schemeShard << "&Action=TablePartitionsFormatSweep" << params;
        runtime.SendToPipe(schemeShard, sender, new NActors::NMon::TEvRemoteHttpInfo(query, HTTP_METHOD_POST), 0, {});
        auto r = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteBinaryInfoRes>(sender);
        NHttp::THttpParser<NHttp::THttpResponse> parser(r->Get()->Blob);
        NHttp::THeaders headers(parser.Headers);
        redirectLocation = headers.Get("Location");
    }
    // `Location` is relative to the original query base, needs adding `/` prefix.
    {
        const TString query = TStringBuilder() << "/" << redirectLocation;
        runtime.SendToPipe(schemeShard, sender, new NActors::NMon::TEvRemoteHttpInfo(query, HTTP_METHOD_GET), 0, {});
        auto r = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
        return r->Get()->Html;
    }
}

// Get table partitions format counters by parsing AdminRequest mon page.
// Sensitive to page markup changes.
TString GetTableFormatCounters(const TString& r) {
    TStringBuf text = r;
    TStringBuf _;
    text.Split("<h4>Status</h4>\n", _, text);
    text.Split("</div>", text, _);
    // skip "Currently:...", return "Tables:..."
    text.NextTok('\n');
    return TString(text.NextTok("<br>"));
}

ui64 GetTablePartitionVersion(TTestActorRuntime& runtime, const TString& path) {
    auto desc = DescribePath(runtime, path);
    return desc.GetPathDescription().GetSelf().GetPathVersion();
}

TPathId CreateFourPartTable(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId, const TString& name) {
    TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "%s"
            Columns { Name: "key"   Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "M" } } } }
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "T" } } } }
            PartitionConfig {
                PartitioningPolicy { MinPartitionsCount: 4 }
            }
        )",
        name.c_str()
    ));
    env.TestWaitNotification(runtime, txId);

    return GetPathId(runtime, "/MyRoot/" + name);
}

}  // namespace

Y_UNIT_TEST_SUITE(TTablePartitionsFormat) {

    Y_UNIT_TEST(FormatSwitchingDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching is not enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(false);

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        CreateFourPartTable(runtime, env, txId, "table");

        {
            const TPathId pathId = GetPathId(runtime, "/MyRoot/dir");
            auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
            UNIT_ASSERT_C(r.Contains("Format switching is disabled"), r);
            r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
            UNIT_ASSERT_C(r.Contains("Format switching is disabled"), r);
        }
        {
            const TPathId pathId = GetPathId(runtime, "/MyRoot/table");
            auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
            UNIT_ASSERT_C(r.Contains("Format switching is disabled"), r);
            r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
            UNIT_ASSERT_C(r.Contains("Format switching is disabled"), r);
        }
        {
            auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
            UNIT_ASSERT_C(r.Contains("ERROR: format switching is disabled"), r);
            r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Pause=1");
            UNIT_ASSERT_C(r.Contains("ERROR: format switching is disabled"), r);
            r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Resume=1");
            UNIT_ASSERT_C(r.Contains("ERROR: format switching is disabled"), r);
            r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Cancel=1");
            UNIT_ASSERT_C(r.Contains("ERROR: format switching is disabled"), r);
        }
    }

    // Switch tests

    Y_UNIT_TEST(SwitchForward) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId pathId = CreateFourPartTable(runtime, env, txId, "Table");
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 0);

        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"C", "M", "T", ""})}
        );

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"C", "M", "T", ""})}
        );
    }

    Y_UNIT_TEST(SwitchForwardThenBack) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId pathId = CreateFourPartTable(runtime, env, txId, "Table");
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 0);

        // Forward: position -> shardidx.
        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 1);

        // Reverse: shardidx -> position.
        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 0);

        // Describe still shows 4 partitions, schema intact.
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"C", "M", "T", ""})}
        );

        // Survive reboot in position format.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
            {NLs::PartitionKeys({"C", "M", "T", ""})}
        );
    }

    Y_UNIT_TEST(SwitchIdempotentNoop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId pathId = CreateFourPartTable(runtime, env, txId, "Table");
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 0);

        // Already in position format: action returns ALREADY_DONE.
        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
        UNIT_ASSERT_C(r.Contains("ALREADY_DONE\n"), r);

        // Switch to shardidx.
        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), 1);

        // Now ALREADY_DONE on shardidx.
        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("ALREADY_DONE\n"), r);
    }

    Y_UNIT_TEST(SwitchDescribeVersionStable) {
        // PartitioningVersion / TablePartitionVersion must NOT bump across a format switch.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId pathId = CreateFourPartTable(runtime, env, txId, "Table");
        const ui64 vBefore = GetTablePartitionVersion(runtime, "/MyRoot/Table");

        auto r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);
        UNIT_ASSERT_VALUES_EQUAL(GetTablePartitionVersion(runtime, "/MyRoot/Table"), vBefore);

        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "position");
        UNIT_ASSERT_C(r.Contains("OK\n"), r);
        UNIT_ASSERT_VALUES_EQUAL(GetTablePartitionVersion(runtime, "/MyRoot/Table"), vBefore);
    }

    // Sweep tests

    Y_UNIT_TEST(SweepOnZeroTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
        UNIT_ASSERT_C(r.Contains("Tables: 0 in <code>position</code>, 0 in <code>shardidx</code>, total 0"), r);

        // Nothing will be run on zero tables
        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        UNIT_ASSERT_C(r.Contains("Start: OK"), r);
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
    }

    Y_UNIT_TEST(SweepOnAllDone) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_VALUES_EQUAL("Tables: 0 in <code>position</code>, 0 in <code>shardidx</code>, total 0", GetTableFormatCounters(r));

        const TPathId p1 = CreateFourPartTable(runtime, env, txId, "T1");

        // Switch format manually
        r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p1, "shardidx");

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
        UNIT_ASSERT_VALUES_EQUAL("Tables: 0 in <code>position</code>, 1 in <code>shardidx</code>, total 1", GetTableFormatCounters(r));

        // Try to run sweep when all tables already converted
        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        UNIT_ASSERT_C(r.Contains("Start: OK"), r);
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
        UNIT_ASSERT_VALUES_EQUAL("Tables: 0 in <code>position</code>, 1 in <code>shardidx</code>, total 1", GetTableFormatCounters(r));
    }

    Y_UNIT_TEST(SweepSkipsConverted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId p1 = CreateFourPartTable(runtime, env, txId, "T1");
        const TPathId p2 = CreateFourPartTable(runtime, env, txId, "T2");
        const TPathId p3 = CreateFourPartTable(runtime, env, txId, "T3");

        // Pre-convert T1 and T2 to ShardIdx.
        PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p1, "shardidx");
        PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p2, "shardidx");

        // Start sweep targeting ShardIdx.
        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        runtime.SimulateSleep(TDuration::Seconds(10));

        // All 3 tables in ShardIdx — 2 skipped (pre-converted), 1 done.
        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

        UNIT_ASSERT_C(
            PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p3, "shardidx").Contains("ALREADY_DONE\n"),
            "T3 not in ShardIdx after sweep"
        );
    }

    Y_UNIT_TEST(SweepConvergesAllTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        const TPathId p1 = CreateFourPartTable(runtime, env, txId, "T1");
        const TPathId p2 = CreateFourPartTable(runtime, env, txId, "T2");
        const TPathId p3 = CreateFourPartTable(runtime, env, txId, "T3");

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        UNIT_ASSERT_C(r.Contains("Start: OK"), r);
        UNIT_ASSERT_C(r.Contains("Currently: <code>Running</code>"), r);

        // Let sweep steps fire (100ms adaptive delay, simulated time).
        runtime.SimulateSleep(TDuration::Seconds(10));

        // All 3 tables should now be in shardidx format.
        UNIT_ASSERT_C(
            PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p1, "shardidx").Contains("ALREADY_DONE\n"),
            "T1 not in ShardIdx after sweep"
        );
        UNIT_ASSERT_C(
            PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p2, "shardidx").Contains("ALREADY_DONE\n"),
            "T2 not in ShardIdx after sweep"
        );
        UNIT_ASSERT_C(
            PostSwitchAction(runtime, TTestTxConfig::SchemeShard, p3, "shardidx").Contains("ALREADY_DONE\n"),
            "T3 not in ShardIdx after sweep"
        );

        // Status must be IDLE.
        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
    }

    Y_UNIT_TEST(SweepPauseResume) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");
        CreateFourPartTable(runtime, env, txId, "T3");

        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Pause=1");
        UNIT_ASSERT_C(r.Contains("Pause: OK"), r);
        UNIT_ASSERT_C(r.Contains("Currently: <code>Paused</code>"), r);

        // Advance time — no progress expected while paused.
        runtime.SimulateSleep(TDuration::Seconds(5));

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Paused</code>"), r);

        // Resume and let finish.
        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Resume=1");
        UNIT_ASSERT_C(r.Contains("Resume: OK"), r);

        runtime.SimulateSleep(TDuration::Seconds(10));

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
    }

    Y_UNIT_TEST(SweepCancel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");

        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Cancel=1");
        UNIT_ASSERT_C(r.Contains("Cancel: OK"), r);
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

        // Advance time — no further progress expected.
        runtime.SimulateSleep(TDuration::Seconds(5));

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
    }

    Y_UNIT_TEST(SweepRestartResume) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");
        CreateFourPartTable(runtime, env, txId, "T3");

        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        // Let one step fire.
        runtime.SimulateSleep(TDuration::MilliSeconds(200));

        // Reboot mid-sweep — sweep should resume automatically.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Let the resumed sweep finish.
        runtime.SimulateSleep(TDuration::Seconds(10));

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

        // All tables must be in ShardIdx format.
        for (const TString& name : {"T1", "T2", "T3"}) {
            const TPathId pathId = GetPathId(runtime, "/MyRoot/" + name);
            r = PostSwitchAction(runtime, TTestTxConfig::SchemeShard, pathId, "shardidx");
            UNIT_ASSERT_C(r.Contains("ALREADY_DONE\n"), name << " not in ShardIdx format after restart-resume");
        }
    }

    Y_UNIT_TEST(SweepSweepVersionStable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");

        const ui64 v1Before = GetTablePartitionVersion(runtime, "/MyRoot/T1");
        const ui64 v2Before = GetTablePartitionVersion(runtime, "/MyRoot/T2");

        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        runtime.SimulateSleep(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(GetTablePartitionVersion(runtime, "/MyRoot/T1"), v1Before);
        UNIT_ASSERT_VALUES_EQUAL(GetTablePartitionVersion(runtime, "/MyRoot/T2"), v2Before);
    }

    Y_UNIT_TEST(SweepAutoCancelOnFlagChange) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // To be independent from current flag defaults
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");
        CreateFourPartTable(runtime, env, txId, "T3");

        // Start sweep targeting ShardIdx.
        PostSweepAction(runtime, TTestTxConfig::SchemeShard, "&Start=1&format=shardidx");
        // Let one step fire.
        runtime.SimulateSleep(TDuration::MilliSeconds(200));

        // Flip the feature flag to opposite — sweep should self-cancel at next step.
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(false);
        // Wait for the self-cancel step to fire.
        runtime.SimulateSleep(TDuration::Seconds(2));

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
    }

    Y_UNIT_TEST(SweepAutoOnBoot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Format switching disabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(false);
        // Auto sweep disabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Table creates in position format
        CreateFourPartTable(runtime, env, txId, "T1");
        CreateFourPartTable(runtime, env, txId, "T2");
        CreateFourPartTable(runtime, env, txId, "T3");

        // Format switching enabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(true);

        // Reboot without autosweep should do nothing
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
        UNIT_ASSERT_C(r.Contains("Tables: 3 in <code>position</code>, 0 in <code>shardidx</code>, total 3"), r);

        // Flip the autosweep feature flag on — sweep should self-start at next reboot
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(true);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Running</code>"), r);
        UNIT_ASSERT_C(r.Contains("Tables: 2 in <code>position</code>, 1 in <code>shardidx</code>, total 3"), r);

        runtime.SimulateSleep(TDuration::Seconds(1));

        r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);
        UNIT_ASSERT_C(r.Contains("Tables: 0 in <code>position</code>, 3 in <code>shardidx</code>, total 3"), r);
    }

    // Format counters tests

    // Table partition format upon creation depends on both
    // EnableTablePartitionsFormatShardIdx and EnableTablePartitionsFormatShardIdxByDefault enabled
    Y_UNIT_TEST_FLAGS(CreateTable, FormatShardIdx, FormatShardIdxByDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Auto sweep disabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(FormatShardIdx);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(FormatShardIdxByDefault);

        // New table must be in shardidx if EnableTablePartitionsFormatShardIdx{,ByDefault} are both set
        const ui32 expectedShardIdxTables = ((FormatShardIdx && FormatShardIdxByDefault) ? 1 : 0);

        // Test body
        CreateFourPartTable(runtime, env, ++txId, "T1");

        // Tablet counters
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatPosition"), 1 - expectedShardIdxTables);
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, "SchemeShard/TablesInFormatShardidx"), expectedShardIdxTables);

        // Admin page counters
        auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
        UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

        TString expectedCounts = Sprintf("Tables: %d in <code>position</code>, %d in <code>shardidx</code>, total 1",
            1 - expectedShardIdxTables,
            expectedShardIdxTables
        );
        UNIT_ASSERT_VALUES_EQUAL(expectedCounts, GetTableFormatCounters(r));
    }

    // CopyTable retains format of the original regardless of flags
    Y_UNIT_TEST_FLAGS_N(CopyTable, bool OriginalInShardIdx, bool FormatShardIdx, bool FormatShardIdxByDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Auto sweep disabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Set format for the original table
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(OriginalInShardIdx);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(OriginalInShardIdx);

        // Create original table
        CreateFourPartTable(runtime, env, ++txId, "T1");

        // Check original table format
        {
            auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
            UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

            ui32 expectedShardIdxTables = (OriginalInShardIdx ? 1 : 0);

            TString expectedCounts = Sprintf("Tables: %d in <code>position</code>, %d in <code>shardidx</code>, total 1",
                1 - expectedShardIdxTables,
                expectedShardIdxTables
            );
            UNIT_ASSERT_VALUES_EQUAL(expectedCounts, GetTableFormatCounters(r));
        }

        // Test body
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(FormatShardIdx);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(FormatShardIdxByDefault);

        TestCopyTable(runtime, ++txId, "/MyRoot", "T1-copy", "/MyRoot/T1");
        env.TestWaitNotification(runtime, txId);

        {
            auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
            UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

            ui32 expectedShardIdxTables = (OriginalInShardIdx ? 2 : 0);

            TString expectedCounts = Sprintf("Tables: %d in <code>position</code>, %d in <code>shardidx</code>, total 2",
                2 - expectedShardIdxTables,
                expectedShardIdxTables
            );
            UNIT_ASSERT_VALUES_EQUAL(expectedCounts, GetTableFormatCounters(r));
        }
    }

    // MoveTable retains format of the original regardless of flags
    Y_UNIT_TEST_FLAGS_N(MoveTable, bool OriginalInShardIdx, bool FormatShardIdx, bool FormatShardIdxByDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Auto sweep disabled
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Set format for the original table
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(OriginalInShardIdx);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(OriginalInShardIdx);

        // Create original table
        CreateFourPartTable(runtime, env, ++txId, "T1");

        // Check original table format
        {
            auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
            UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

            ui32 expectedShardIdxTables = (OriginalInShardIdx ? 1 : 0);

            TString expectedCounts = Sprintf("Tables: %d in <code>position</code>, %d in <code>shardidx</code>, total 1",
                1 - expectedShardIdxTables,
                expectedShardIdxTables
            );
            UNIT_ASSERT_VALUES_EQUAL(expectedCounts, GetTableFormatCounters(r));
        }

        // Test body
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdx(FormatShardIdx);
        runtime.GetAppData().FeatureFlags.SetEnableTablePartitionsFormatShardIdxByDefault(FormatShardIdxByDefault);

        TestMoveTable(runtime, ++txId, "/MyRoot/T1", "/MyRoot/T1-moved");
        env.TestWaitNotification(runtime, txId);

        {
            auto r = PostSweepAction(runtime, TTestTxConfig::SchemeShard, "");
            UNIT_ASSERT_C(r.Contains("Currently: <code>Idle</code>"), r);

            ui32 expectedShardIdxTables = (OriginalInShardIdx ? 1 : 0);

            TString expectedCounts = Sprintf("Tables: %d in <code>position</code>, %d in <code>shardidx</code>, total 1",
                1 - expectedShardIdxTables,
                expectedShardIdxTables
            );
            UNIT_ASSERT_VALUES_EQUAL(expectedCounts, GetTableFormatCounters(r));
        }
    }

}
