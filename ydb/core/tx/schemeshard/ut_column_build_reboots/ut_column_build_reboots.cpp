#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/column_build.h>
#include <ydb/core/tx/schemeshard/index/column_build_test_hooks.h>
#include <util/generic/scope.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

std::unique_ptr<TEvIndexBuilder::TEvCreateRequest> CreateMixedColumnsRequest(ui64 txId) {
    NKikimrIndexBuilder::TIndexBuildSettings settings;
    settings.set_source_path("/MyRoot/Table");
    settings.MutableScanSettings()->SetMaxBatchRows(1);
    settings.set_max_shards_in_flight(2);
    for (const auto& [name, type] : TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>{
            {"plain_number", Ydb::Type::UINT64}, {"default_number", Ydb::Type::UINT32},
            {"plain_text", Ydb::Type::UTF8}, {"default_text", Ydb::Type::UTF8}}) {
        auto* column = settings.mutable_column_build_operation()->add_column();
        column->SetColumnName(name);
        column->MutableColumnType()->set_type_id(type);
        if (name.StartsWith("default_")) {
            auto* literal = column->mutable_default_from_literal();
            *literal->mutable_type() = column->GetColumnType();
            if (name == "default_number") {
                literal->mutable_value()->set_uint32_value(10);
            } else {
                literal->mutable_value()->set_text_value("default");
            }
        }
    }
    return std::make_unique<TEvIndexBuilder::TEvCreateRequest>(txId, "/MyRoot", settings);
}

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
    Y_UNIT_TEST_WITH_REBOOTS(MixedColumns) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            auto request = CreateMixedColumnsRequest(++t.TxId);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TInactiveZone inactive(activeZone);
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value", "plain_number", "plain_text", "default_number", "default_text"}, {}, {"key"}, true),
                [](const auto& description) {
                    for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
                        UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.GetName());
                    }
                }
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MixedColumnsCancelling) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            const ui64 buildId = ++t.TxId;
            auto request = CreateMixedColumnsRequest(buildId);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            TestCancelBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            t.TestEnv->TestWaitNotification(runtime, buildId);

            TInactiveZone inactive(activeZone);
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MixedColumnsRejecting) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            size_t injected = 0;
            auto backfillFailure = runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
                auto& record = ev->Get()->Record;
                if (record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta()) {
                    ++injected;
                    record.SetStatus(NKikimrIndexBuilder::BUILD_ERROR);
                    record.SetUploadStatus(Ydb::StatusIds::GENERIC_ERROR);
                }
            });
            auto request = CreateMixedColumnsRequest(++t.TxId);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TInactiveZone inactive(activeZone);
            UNIT_ASSERT_C(injected > 0, "Expected a failed backfill batch");
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)
            });
        });
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

            // Fail after an actual uploaded backfill batch.
            auto backfillFailure = runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([](auto& ev) {
                auto& record = ev->Get()->Record;
                if (record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta()) {
                    record.SetStatus(NKikimrIndexBuilder::BUILD_ERROR);
                    record.SetUploadStatus(Ydb::StatusIds::GENERIC_ERROR);
                }
            });
            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::JSON);
            columnDefaultValue.mutable_value()->set_text_value("{\"value\":1}");

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

Y_UNIT_TEST_SUITE(ColumnBuildVisibilityTest) {
    Y_UNIT_TEST(ColumnPropertiesArePersisted) {
        NColumnBuildTest::TFixture f(true, true);
        const ui64 id = ++f.TxId;
        auto request = CreateMixedColumnsRequest(id);
        auto* settings = request->Record.MutableSettings();
        auto* requestedColumns = settings->mutable_column_build_operation();
        requestedColumns->mutable_column(1)->SetNotNull(true);
        requestedColumns->mutable_column(0)->SetFamily("cold");
        requestedColumns->mutable_column(3)->SetFamily("cold");
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge, request.release());
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        f.AssertOriginal();
        const auto result = TestGetBuildIndex(f.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", id);
        const auto& columns = result.GetIndexBuild().GetSettings().column_build_operation();
        UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(columns.column(0).GetColumnName(), "plain_number");
        UNIT_ASSERT_VALUES_EQUAL(columns.column(0).GetFamily(), "cold");
        UNIT_ASSERT(!columns.column(0).has_default_from_literal());
        UNIT_ASSERT_VALUES_EQUAL(columns.column(1).GetColumnName(), "default_number");
        UNIT_ASSERT(columns.column(1).GetNotNull());
        UNIT_ASSERT_VALUES_EQUAL(columns.column(3).GetFamily(), "cold");
        const auto description = DescribePath(f.Runtime, "/MyRoot/Table");
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT(!column.GetIsBuildInProgress());
            if (column.GetName() == "default_number") {
                UNIT_ASSERT(column.GetNotNull());
            }
            if (column.GetName() == "plain_number" || column.GetName() == "default_text") {
                UNIT_ASSERT_VALUES_EQUAL(column.GetFamily(), 1);
            }
        }
    }
    Y_UNIT_TEST(MixedColumnsStayHiddenUntilCompletion) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime);
        auto request = CreateMixedColumnsRequest(++txId);
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
        runtime.WaitFor("column build scan", [&] { return !blocker.empty(); }, TDuration::Minutes(5));

        auto description = DescribePath(runtime, "/MyRoot/Table");
        const auto& columns = description.GetPathDescription().GetTable().GetColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 6);
        for (const auto& column : columns) {
            const bool added = column.GetName() != "key" && column.GetName() != "value";
            UNIT_ASSERT_VALUES_EQUAL_C(column.GetIsBuildInProgress(), added, column.GetName());
        }

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId);
        description = DescribePath(runtime, "/MyRoot/Table");
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.GetName());
        }
    }

}

namespace {
using namespace NColumnBuildTest;
enum class ERebootPoint { Persisted, SchemaCreated, Filling, Cancelling };

class TSeedLegacyColumnRows final : public ISSDataProcessor {
    class TTxSeed final : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
        ui64 BuildId;
        bool MissingValue;
    public:
        TTxSeed(TSchemeShard* ss, ui64 id, bool missingValue)
            : TTransactionBase(ss)
            , BuildId(id)
            , MissingValue(missingValue)
        {}

        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            for (ui64 i = 0; i < 3; ++i) {
                Ydb::TypedValue literal;
                literal.mutable_type()->set_type_id(Ydb::Type::JSON);
                if (i != 1 || !MissingValue) {
                    literal.mutable_value()->set_text_value(i == 1 ? "not [json]" : "{}");
                }
                db.Table<Schema::BuildColumnOperationSettings>().Key(TIndexBuildId(BuildId), i)
                    .Update<Schema::BuildColumnOperationSettings::DefaultFromLiteral>(literal.SerializeAsString())
                    .UpdateToNull<Schema::BuildColumnOperationSettings::ColumnTypeProto,
                        Schema::BuildColumnOperationSettings::HasDefaultFromLiteral>();
            }
            return true;
        }

        void Complete(const TActorContext&) override { Committed = true; }
    };

public:
    static inline bool Committed = false;
    ui64 BuildId = 0;
    bool MissingValue = false;
    TString GetClassName() const override { return "ColumnBuildTest.SeedLegacyColumnRows"; }
    TString DebugString() const override { return GetClassName(); }
    TString DoSerializeToString() const override { return ToString(BuildId) + ":" + ToString(MissingValue); }
    bool DoDeserializeFromString(const TString& data) override {
        TStringBuf id, missing;
        return TStringBuf(data).TrySplit(':', id, missing) && TryFromString(id, BuildId) && TryFromString(missing, MissingValue);
    }
    void DoProcess(TSchemeShard& ss, NKikimrScheme::TEvProcessingResponse&) const override {
        const auto& ctx = TActivationContext::AsActorContext();
        ctx.Send(ctx.SelfID, new NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction(
            std::make_unique<TTxSeed>(&ss, BuildId, MissingValue)));
    }
    static inline const TFactory::TRegistrator<TSeedLegacyColumnRows> Registrator{"ColumnBuildTest.SeedLegacyColumnRows"};
};

void CheckLegacyInvalidLiteralRestoration(bool terminal, bool missingValue) {
    TFixture f(true, true);
    NKikimrIndexBuilder::TIndexBuildSettings settings;
    settings.set_source_path("/MyRoot/Table");
    settings.MutableScanSettings()->SetMaxBatchRows(1);
    for (size_t i = 0; i < 3; ++i) {
        auto* column = settings.mutable_column_build_operation()->add_column();
        column->SetColumnName(TStringBuilder() << "legacy" << i);
        column->MutableColumnType()->set_type_id(Ydb::Type::JSON);
        *column->mutable_default_from_literal()->mutable_type() = column->GetColumnType();
        column->mutable_default_from_literal()->mutable_value()->set_text_value("{}");
    }
    TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(f.Runtime, [&](const auto&) { return !terminal; });
    auto failure = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
        auto& record = ev->Get()->Record;
        if (terminal && record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta()) {
            record.SetStatus(NKikimrIndexBuilder::BUILD_ERROR);
            record.SetUploadStatus(Ydb::StatusIds::GENERIC_ERROR);
        }
    });
    const auto start = [&] {
        const auto id = ++f.TxId;
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", settings));
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        return id;
    };
    const auto id = start();
    if (terminal) {
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
    } else {
        f.Runtime.WaitFor("legacy in-flight schema", [&] { return !scans.empty(); }, TDuration::Minutes(5));
    }
    failure.Remove();
    TSeedLegacyColumnRows seed;
    seed.BuildId = id;
    seed.MissingValue = missingValue;
    TSeedLegacyColumnRows::Committed = false;
    const auto edge = f.Runtime.AllocateEdgeActor();
    ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge, new TEvSchemeShard::TEvProcessingRequest(seed));
    f.Runtime.GrabEdgeEvent<TEvSchemeShard::TEvProcessingResponse>(edge);
    f.Runtime.WaitFor("legacy rows committed", [] { return TSeedLegacyColumnRows::Committed; }, TDuration::Minutes(5));
    RebootTablet(f.Runtime, TTestTxConfig::SchemeShard, f.Runtime.AllocateEdgeActor());
    const auto restored = InspectBuild(f.Runtime, id, terminal);
    const auto& columns = restored.GetSettings().column_build_operation();
    UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), 3);
    for (size_t i = 0; i < 3; ++i) {
        const auto& column = columns.column(i);
        UNIT_ASSERT_VALUES_EQUAL(column.GetColumnName(), TStringBuilder() << "legacy" << i);
        UNIT_ASSERT_VALUES_EQUAL(ui32(column.GetColumnType().type_id()), ui32(Ydb::Type::JSON));
        UNIT_ASSERT(column.has_default_from_literal());
        UNIT_ASSERT_VALUES_EQUAL(column.default_from_literal().has_value(), i != 1 || !missingValue);
        if (column.default_from_literal().has_value()) {
            UNIT_ASSERT_VALUES_EQUAL(column.default_from_literal().value().text_value(), i == 1 ? "not [json]" : "{}");
        }
    }
    if (!terminal) {
        TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
        scans.Stop().Unblock();
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
    } else {
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
    }
    f.AssertSchema({});
    f.AssertOriginal();
    InspectBuild(f.Runtime, id);
    f.Wait(start(), Ydb::Table::IndexBuildState::STATE_DONE);
    f.AssertOriginal();
}

void CheckBatchReboot(ERebootPoint point, bool withData, bool multiShard) {
    // Each family and each of the six mixed kind patterns, with distinct carriers.
    const TVector<TOrder> orders{
        {"n1", "n2", "n3"}, {"d1", "d2", "d3"},
        {"n1", "n2", "d1", "d2"}, {"n1", "d1", "n2", "d2"},
        {"n1", "d1", "d2", "n2"}, {"d1", "n1", "n2", "d2"},
        {"d1", "n1", "d2", "n2"}, {"d1", "d2", "n1", "n2"}};
    for (const auto& order : orders) {
        TFixture f(withData, multiShard);
        const bool defaults = AnyOf(order, [](const auto& name) { return name.StartsWith('d'); });
        size_t batches = 0;
        auto progress = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            batches += record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta() > 0;
        });
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> schema(f.Runtime, [&](const auto& ev) {
            const auto type = ev->Get()->Record.GetTransaction(0).GetOperationType();
            return (point == ERebootPoint::Persisted && type == NKikimrSchemeOp::ESchemeOpCreateLock)
                || (point == ERebootPoint::SchemaCreated && type == NKikimrSchemeOp::ESchemeOpCreateColumnBuild)
                || (point == ERebootPoint::Cancelling && type == NKikimrSchemeOp::ESchemeOpDropColumnBuild);
        });
        size_t paused = 0;
        TIndexBuildInfo::EState pausedState = TIndexBuildInfo::EState::Invalid;
        TColumnBuildTestHooks::BeforeFillingProgress = [&](const TIndexBuildInfo& build) {
            if (defaults || (point != ERebootPoint::Filling && point != ERebootPoint::Cancelling)) {
                return false;
            }
            ++paused;
            pausedState = build.State;
            return true;
        };
        Y_DEFER { TColumnBuildTestHooks::BeforeFillingProgress = {}; };
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(f.Runtime, [&](const auto&) {
            return defaults && ((point == ERebootPoint::Filling && !withData) || point == ERebootPoint::Cancelling);
        });
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(f.Runtime, [&](const auto&) {
            return defaults && withData && point == ERebootPoint::Filling;
        });
        ui64 id;
        if (point == ERebootPoint::Persisted) {
            // Creation replies after taking the lock, which is held at this barrier.
            id = ++f.TxId;
            ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, f.Runtime.AllocateEdgeActor(),
                new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", Settings(order)));
        } else {
            id = f.Start(order);
        }
        f.Runtime.WaitFor("reboot barrier", [&] { return !schema.empty() || !scans.empty() || !uploads.empty() || paused != 0; }, TDuration::Minutes(5));
        if (point == ERebootPoint::Filling && defaults && withData) {
            uploads.Unblock(1);
            f.Runtime.WaitFor("persisted backfill progress", [&] { return batches > 0 && !uploads.empty(); }, TDuration::Minutes(5));
            // A get request is processed after the progress response, ensuring its transaction completed.
        }
        if (point == ERebootPoint::Cancelling) {
            TColumnBuildTestHooks::BeforeFillingProgress = {};
            TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
            scans.Stop().Unblock();
            f.Runtime.WaitFor("cancellation rollback proposal", [&] { return !schema.empty(); }, TDuration::Minutes(5));
        }
        if (!defaults && (point == ERebootPoint::Filling || point == ERebootPoint::Cancelling)) {
            UNIT_ASSERT(paused != 0 && pausedState == TIndexBuildInfo::EState::Filling);
        }
        Inspect(f.Runtime, id, order, false);
        paused = 0;
        RebootTablet(f.Runtime, TTestTxConfig::SchemeShard, f.Runtime.AllocateEdgeActor());
        Inspect(f.Runtime, id, order, false);
        if (!defaults && point == ERebootPoint::Filling) {
            UNIT_ASSERT(paused != 0 && pausedState == TIndexBuildInfo::EState::Filling);
        }
        if (point != ERebootPoint::Persisted) {
            f.AssertSchema(order, true);
        } else {
            f.AssertSchema({});
        }
        schema.Stop().Unblock();
        scans.Stop().Unblock();
        TColumnBuildTestHooks::BeforeFillingProgress = {};
        if (!defaults && point == ERebootPoint::Filling) {
            ResumeBuild(f.Runtime, id);
        }
        uploads.Stop().Unblock();
        if (point == ERebootPoint::Cancelling) {
            f.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
            f.AssertSchema({});
            f.AssertOriginal();
            Inspect(f.Runtime, id, order);
            f.Retry(order);
        } else {
            f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
            f.AssertSchema(order);
            f.AssertValues(order);
            Inspect(f.Runtime, id, order);
        }
    }
}
}

Y_UNIT_TEST_SUITE(ColumnBuildBatchRebootsTest) {
    Y_UNIT_TEST_FLAGS(LegacyInvalidLiteralRestoration, Terminal, MissingValue) { CheckLegacyInvalidLiteralRestoration(Terminal, MissingValue); }
    Y_UNIT_TEST_FLAGS(Persisted, WithData, MultiShard) { CheckBatchReboot(ERebootPoint::Persisted, WithData, MultiShard); }
    Y_UNIT_TEST_FLAGS(SchemaCreated, WithData, MultiShard) { CheckBatchReboot(ERebootPoint::SchemaCreated, WithData, MultiShard); }
    Y_UNIT_TEST_FLAGS(Filling, WithData, MultiShard) { CheckBatchReboot(ERebootPoint::Filling, WithData, MultiShard); }
    Y_UNIT_TEST_FLAGS(Cancelling, WithData, MultiShard) { CheckBatchReboot(ERebootPoint::Cancelling, WithData, MultiShard); }
}
