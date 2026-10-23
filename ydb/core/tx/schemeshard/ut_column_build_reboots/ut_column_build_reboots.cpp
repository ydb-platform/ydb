#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

template <typename T>
void CreateSampleTableWithData(T& t, TTestActorRuntime& runtime, bool& activeZone) {
    auto fnWriteRow = [&] (ui64 tabletId, ui32 key, TString value, const char* table) {
        TString writeQuery = Sprintf(R"(
            (
                (let key   '( '('key   (Uint32 '%u ) ) ) )
                (let row   '( '('value (Utf8 '%s) ) ) )
                (return (AsList (UpdateRow '__user__%s key row) ))
            )
        )", key, value.c_str(), table);
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
        UNIT_ASSERT_VALUES_EQUAL(err, "");
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
    };

    {
        TInactiveZone inactive(activeZone);

        TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");

        t.TestEnv->TestWaitNotification(runtime, t.TxId);

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, "value", "Table");
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(ColumnBuildRebootsTest) {
    Y_UNIT_TEST(GeneratedBackfillReboots) {
        // Empty shards isolate durable orchestration from scalar evaluation;
        // DataShard tests cover row values at the same recovery boundaries.
        for (bool beforePublication : {false, true}) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime);
            runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
            runtime.GetAppData().FeatureFlags.SetEnableAddColumnGeneratedStored(true);
            runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            ui64 txId = 100;
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(Name: "Table" Columns { Name: "key" Type: "Uint32" } KeyColumnNames: ["key"] UniformPartitionsCount: 2)");
            env.TestWaitNotification(runtime, txId);
            TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(runtime);
            TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> publication(runtime, [&](const auto& ev) {
                return beforePublication && ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
            });
            auto request = CreateGeneratedBuildColumnRequest(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table");
            const ui64 buildId = txId;
            const auto expected = request->Record.GetSettings().column_build_operation().column(0).generated_from_expression().SerializeAsString();
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            runtime.WaitFor("generated shards ready", [&] { return scans.size() >= 2; });
            auto complete = [&](const TEvDataShard::TEvBuildIndexCreateRequest::TPtr& scan) {
                const auto& source = scan->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(source.GetColumnBuildSettings().column(0).generated_from_expression().SerializeAsString(), expected);
                auto reply = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
                auto& record = reply->Record;
                record.SetId(source.GetId());
                record.SetTabletId(source.GetTabletId());
                record.SetRequestSeqNoGeneration(source.GetSeqNoGeneration());
                record.SetRequestSeqNoRound(source.GetSeqNoRound());
                record.SetStatus(NKikimrIndexBuilder::DONE);
                record.SetUploadStatus(Ydb::StatusIds::SUCCESS);
                runtime.Send(new IEventHandle(scan->Sender, runtime.AllocateEdgeActor(), reply.Release()));
            };
            const auto originalGeneration = scans.front()->Get()->Record.GetSeqNoGeneration();
            const auto completedTablet = scans.front()->Get()->Record.GetTabletId();
            complete(scans.front());
            scans.pop_front();
            const auto partial = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            UNIT_ASSERT_VALUES_EQUAL_C(partial.GetIndexBuild().GetProgress(), 50.0, partial.DebugString());
            const auto remainingTablet = scans.front()->Get()->Record.GetTabletId();
            scans.clear();
            RebootTablet(runtime, remainingTablet, runtime.AllocateEdgeActor());
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            runtime.WaitFor("remaining generated shard resumed", [&] {
                while (!scans.empty() && scans.front()->Get()->Record.GetSeqNoGeneration() <= originalGeneration) {
                    scans.pop_front();
                }
                return !scans.empty();
            });
            UNIT_ASSERT_VALUES_UNEQUAL(scans.front()->Get()->Record.GetTabletId(), completedTablet);
            complete(scans.front());
            scans.pop_front();
            if (beforePublication) {
                runtime.WaitFor("generated publication held", [&] { return !publication.empty(); });
                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
                const auto persisted = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
                UNIT_ASSERT_VALUES_EQUAL(persisted.GetIndexBuild().GetSettings().column_build_operation().column(0).generated_from_expression().SerializeAsString(), expected);
            }
            publication.Stop().Unblock();
            env.TestWaitNotification(runtime, buildId);
            scans.Stop();
            RebootTablet(runtime, completedTablet, runtime.AllocateEdgeActor());
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            const auto done = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            UNIT_ASSERT_VALUES_EQUAL_C(done.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, done.DebugString());
            bool foundGenerated = false;
            const auto description = DescribePath(runtime, "/MyRoot/Table").GetPathDescription();
            for (const auto& column : description.GetTable().GetColumns()) {
                if (column.GetName() == "g") {
                    foundGenerated = true;
                    UNIT_ASSERT(!column.GetIsBuildInProgress());
                    UNIT_ASSERT(column.GetDefaultFromExpression().GetStored());
                    UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromExpression().GetExprText(), "key + 1");
                }
            }
            UNIT_ASSERT(foundGenerated);
        }
    }

    Y_UNIT_TEST(GeneratedDescriptorRoundTrip) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        runtime.GetAppData().FeatureFlags.SetEnableAddColumnGeneratedStored(true);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> lock(runtime, [&](const auto& ev) {
            return ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock;
        });
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(runtime);
        auto request = CreateGeneratedBuildColumnRequest(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table");
        const auto expected = request->Record.GetSettings().column_build_operation().column(0).generated_from_expression().SerializeAsString();
        const ui64 buildId = txId;
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
        runtime.WaitFor("generated build persisted", [&] { return !lock.empty(); });
        auto checkDescriptor = [&] {
            const auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            const auto& settings = result.GetIndexBuild().GetSettings().column_build_operation();
            UNIT_ASSERT_VALUES_EQUAL(settings.column_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(settings.column(0).generated_from_expression().SerializeAsString(), expected);
            UNIT_ASSERT(!settings.column(0).has_default_from_literal());
        };
        checkDescriptor();
        runtime.GetAppData().FeatureFlags.SetEnableAddColumnGeneratedStored(false);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        checkDescriptor();
        lock.Stop().Unblock();
        runtime.WaitFor("generated hidden schema and snapshot installed", [&] { return !scans.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(scans.front()->Get()->Record.GetColumnBuildSettings().column(0).generated_from_expression().SerializeAsString(), expected);
        // A second restart resumes the persisted descriptor after the source
        // version has legitimately changed through hidden-column installation.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        checkDescriptor();
        RebootTablet(runtime, scans.front()->Get()->Record.GetTabletId(), runtime.AllocateEdgeActor());
        checkDescriptor();
        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
        env.TestWaitNotification(runtime, buildId);
        const auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, result.DebugString());
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::CheckColumns("Table", {"key", "value", "g"}, {"g"}, {"key"}, true)});
        scans.Stop();
        // The accepted build remains cancellable with its acceptance flag off,
        // and its lock is released so later schema operations can proceed.
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(Name: "Table" Columns { Name: "after" Type: "Uint32" })");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_WITH_REBOOTS(BaseCase) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(6),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(Cancelling) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);

            ui64 buildIndexId = t.TxId;
            TestCancelBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(7),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(Rejecting) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            // Set invalid default value to get rejected
            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::JSON);
            columnDefaultValue.mutable_value()->set_text_value("{not json]");

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(7),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
            }
        });
    }
}
