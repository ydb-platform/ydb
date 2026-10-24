#include <ydb/core/tx/datashard/build_index/build_index_scan_manager.h>
#include <ydb/core/tx/datashard/build_index/generated_columns_scan.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

namespace NKikimr::NDataShard {
namespace {
void RunGeneratedScan(bool empty, bool materialized, bool failing, bool expanding = false) {
    using namespace Tests;
    using namespace NKqpHelpers;
    TPortManager pm;
    TServerSettings settings(pm.GetPort(2134));
    settings.SetDomainName("Root").SetUseRealThreads(false);
    TServer::TPtr server = new TServer(settings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);
    CreateShardedTable(server, sender, "/Root", "Table", 1, false);
    if (!empty) {
        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10), (2, 20), (3, 30);");
    }
    const auto source = ResolveTableId(server, sender, "/Root/Table");
    auto install = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& event) {
        auto& record = event->Get()->Record;
        if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
            NKikimrTxDataShard::TFlatSchemeTransaction transaction;
            UNIT_ASSERT(transaction.ParseFromString(record.GetTxBody()));
            if (transaction.HasAlterTable()) {
                for (auto& column : *transaction.MutableAlterTable()->MutableColumns()) {
                    if (column.GetName() == "g") {
                        column.SetIsBuildInProgress(true);
                        column.MutableDefaultFromExpression()->SetStored(true);
                        column.MutableDefaultFromExpression()->SetExprText("42");
                    }
                }
                record.SetTxBody(transaction.SerializeAsString());
            }
        }
    });
    ExecSQL(server, sender, expanding ? "ALTER TABLE `/Root/Table` ADD COLUMN g String;"
        : "ALTER TABLE `/Root/Table` ADD COLUMN g Int64;", false);
    install.Remove();
    if (materialized && !empty) {
        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (key, g) VALUES (1, 11), (2, NULL), (3, 33);");
    }
    const TString before = ReadShardedTable(server, "/Root/Table");
    const auto table = ResolveTableId(server, sender, "/Root/Table");
    const auto shard = GetTableShards(server, sender, "/Root/Table").at(0);
    const auto snapshot = CreateVolatileSnapshot(server, {"/Root/Table"});
    auto request = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
    auto& record = request->Record;
    record.SetId(98765);
    record.SetTabletId(shard);
    record.SetOwnerId(table.PathId.OwnerId);
    record.SetPathId(table.PathId.LocalPathId);
    record.SetDatabaseName("/Root");
    record.SetTargetName("/Root/Table");
    record.SetSeqNoGeneration(1);
    record.SetSeqNoRound(1);
    record.SetSnapshotStep(snapshot.Step);
    record.SetSnapshotTxId(snapshot.TxId);
    record.MutableScanSettings()->SetMaxBatchRows(expanding ? 10 : 1);
    if (expanding) record.MutableScanSettings()->SetMaxBatchBytes(256);
    auto* setting = record.MutableColumnBuildSettings()->add_column();
    setting->SetColumnName("g");
    auto* descriptor = setting->mutable_generated_from_expression();
    descriptor->SetExprText("42");
    descriptor->SetSourceOwnerId(table.PathId.OwnerId);
    descriptor->SetSourcePathId(table.PathId.LocalPathId);
    descriptor->SetSourceSchemaVersion(source.SchemaVersion);
    descriptor->MutableOutputType()->mutable_optional_type()->mutable_item()->set_type_id(expanding ? Ydb::Type::STRING : Ydb::Type::INT64);
    {
        using namespace NMiniKQL;
        const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment environment(alloc);
        TProgramBuilder builder(environment, *registry);
        auto value = expanding
            ? builder.NewOptional(builder.NewDataLiteral<NUdf::EDataSlot::String>(TString(180, 'x')))
            : builder.NewOptional(builder.NewDataLiteral<i64>(42));
        if (failing) {
            value = builder.Ensure(value, builder.NewDataLiteral<bool>(false),
                builder.NewDataLiteral<NUdf::EDataSlot::String>("deliberate generated evaluation failure"), __FILE__, 0, 0);
        }
        descriptor->SetProgram(SerializeRuntimeNode(value, environment));
        descriptor->SetProgramFormatVersion(1);
        descriptor->SetMiniKqlRuntimeVersion(RuntimeVersion.Value());
        descriptor->SetLanguageVersion(NYql::MinLangVersion);
        descriptor->SetRuntimeSettings(NYql::SerializeRuntimeSettingsToString(*NYql::MakeRuntimeSettings()));
    }
    TMaybe<NKikimrIndexBuilder::EBuildStatus> finalStatus;
    TString issue;
    bool acknowledged = false;
    ui32 uploads = 0;
    ui32 committedUploads = 0;
    ui32 finalAck = 0;
    auto committed = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& event) {
        if (event->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK) ++committedUploads;
    });
    auto progress = runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& event) {
        const auto& result = event->Get()->Record;
        if (result.GetId() != 98765) return;
        acknowledged |= result.HasLastKeyAck();
        if (expanding && result.HasLastKeyAck()) {
            const TSerializedCellVec key(result.GetLastKeyAck());
            const ui32 ack = key.GetCells().at(0).AsValue<ui32>();
            UNIT_ASSERT_C(ack <= committedUploads, "Acknowledged unevaluated/uncommitted residual rows");
            finalAck = Max(finalAck, ack);
        }
        if (result.GetStatus() == NKikimrIndexBuilder::DONE || result.GetStatus() == NKikimrIndexBuilder::BUILD_ERROR || result.GetStatus() == NKikimrIndexBuilder::BAD_REQUEST) {
            finalStatus = result.GetStatus();
            issue = result.ShortDebugString();
        }
    });
    auto upload = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto& event) {
        if (event->Get()->Record.HasGeneratedColumnFillContext()) {
            ++uploads;
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Record.RowsSize(), 1);
        }
    });
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.WaitFor("generated scan terminal status", [&] { return finalStatus.Defined(); });
    const bool error = failing && !materialized && !empty;
    UNIT_ASSERT_VALUES_EQUAL_C(*finalStatus, error ? NKikimrIndexBuilder::BUILD_ERROR : NKikimrIndexBuilder::DONE, issue);
    if (empty || materialized || error) {
        UNIT_ASSERT_VALUES_EQUAL(uploads, 0);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/Table"), before);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(uploads, 3);
        UNIT_ASSERT(acknowledged);
    }
    if (materialized && !empty) UNIT_ASSERT(acknowledged);
    if (expanding) UNIT_ASSERT_VALUES_EQUAL(finalAck, 3);
    if (error) UNIT_ASSERT_STRING_CONTAINS(issue, "deliberate generated evaluation failure");
}
} // namespace

Y_UNIT_TEST_SUITE(TTxDataShardGeneratedColumns) {
    Y_UNIT_TEST(EmptyAndAllSkippedRanges) {
        RunGeneratedScan(true, false, false);
        RunGeneratedScan(false, true, false);
    }

    Y_UNIT_TEST(SkipMaterializedWithoutEvaluation) {
        RunGeneratedScan(false, true, true);
    }

    Y_UNIT_TEST(EvaluationFailure) {
        RunGeneratedScan(false, false, true);
    }

    Y_UNIT_TEST(ExpandingOutputBatches) {
        RunGeneratedScan(false, false, false, true);
    }

    Y_UNIT_TEST(EvaluationBackpressure) {
        RunGeneratedScan(false, false, false);
    }

    Y_UNIT_TEST(DependencyProjection) {
        TUserTable table;
        table.Columns.emplace(17, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", "key", true));
        table.Columns.emplace(23, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", "a"));
        table.Columns.emplace(41, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Int64), "", "g"));
        table.KeyColumnIds = {17};
        NKikimrIndexBuilder::TGeneratedColumnBuild descriptor;
        for (const auto& [id, name] : {std::pair{23u, "a"}, {17u, "key"}}) {
            auto* dependency = descriptor.AddDependencies();
            dependency->SetColumnId(id);
            dependency->SetName(name);
            NScheme::ProtoFromTypeInfo(table.Columns.at(id).Type, *dependency->MutableType(), table.Columns.at(id).NotNull);
        }
        const auto projection = BuildGeneratedColumnsProjection(table, descriptor, 41);
        UNIT_ASSERT_VALUES_EQUAL(projection.Tags.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(projection.Tags[0], 23);
        UNIT_ASSERT_VALUES_EQUAL(projection.Tags[1], 41);
        const i64 keyValue = 101;
        const TVector<TCell> key{TCell::Make(keyValue)};
        NTable::TRowState row(2);
        row.Touch(NTable::ERowOp::Upsert);
        row.Set(0, NTable::ECellOp::Set, TCell());
        const auto inputs = projection.GetInputs(key, row);
        UNIT_ASSERT_VALUES_EQUAL(inputs.size(), 2);
        UNIT_ASSERT(inputs[0].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(inputs[1].AsValue<i64>(), 101);
        descriptor.MutableDependencies(0)->SetColumnId(24);
        UNIT_ASSERT_EXCEPTION(BuildGeneratedColumnsProjection(table, descriptor, 41), yexception);
    }

    Y_UNIT_TEST(GeneratedFillNullSurvivesCompaction) {
        using namespace NTable;
        using namespace NTable::NTest;
        TDbExec database;
        TAlter alter;
        alter.AddTable("rows", 1)
            .AddColumn(1, "key", 1, ETypes::Uint64, false, false)
            .AddColumn(1, "target", 41, ETypes::Uint64, false, false)
            .AddColumnToKey(1, 1);
        database.Begin().Apply(*alter.Flush()).Commit();
        const auto missing = *database.SchemedCookRow(1).Col(ui64(1), ECellOp::Empty);
        const auto explicitNull = *database.SchemedCookRow(1).Col(ui64(2), ECellOp::Null);
        const auto value = *database.SchemedCookRow(1).Col(ui64(3), ui64(99));
        database.Begin().Put(1, missing, explicitNull, value).Commit();
        auto check = [&] {
            database.ReadVer(TRowVersion::Max());
            const TVector<TTag> tags{41};
            for (ui64 key = 1; key <= 3; ++key) {
                const TVector<TRawTypeValue> keys{TRawTypeValue(&key, sizeof(key), NScheme::NTypeIds::Uint64)};
                TRowState row;
                UNIT_ASSERT(database->Select(1, keys, tags, row, 0) == EReady::Data);
                UNIT_ASSERT(row == ERowOp::Upsert);
                if (key == 1) {
                    UNIT_ASSERT(row.GetCellOp(0) == ECellOp::Empty);
                } else {
                    UNIT_ASSERT(row.GetCellOp(0) != ECellOp::Empty);
                    UNIT_ASSERT_VALUES_EQUAL(row.Get(0).IsNull(), key == 2);
                }
            }
            database.Cleanup();
        };
        check();
        database.Snap(1).Compact(1, true);
        check();
        database.Replay(EPlay::Boot);
        check();
        database.Replay(EPlay::Redo);
        check();
    }

    Y_UNIT_TEST(SkipMaterializedAtSnapshot) {
        NTable::TRowState absent(1);
        absent.Touch(NTable::ERowOp::Upsert);
        UNIT_ASSERT(absent.GetCellOp(0) == NTable::ECellOp::Empty);
        NTable::TRowState explicitNull(1);
        explicitNull.Touch(NTable::ERowOp::Upsert);
        explicitNull.Set(0, NTable::ECellOp::Set, TCell());
        UNIT_ASSERT(explicitNull.Get(0).IsNull());
        UNIT_ASSERT(explicitNull.GetCellOp(0) != NTable::ECellOp::Empty);
    }

    Y_UNIT_TEST(GeneratedFillContextSurvivesRebootAndRevocation) {
        NTable::NTest::TDbExec database;
        // Exercise the manager against its persisted storage contract without
        // exposing DataShard's private tablet schema to the test.
        NTable::TAlter alter;
        alter.AddTable("IndexBuildScans", 39)
            .AddColumn(39, "BuildId", 1, NScheme::NTypeIds::Uint64, false, false)
            .AddColumn(39, "SeqNoGeneration", 2, NScheme::NTypeIds::Uint64, false, false)
            .AddColumn(39, "SeqNoRound", 3, NScheme::NTypeIds::Uint64, false, false)
            .AddColumn(39, "ResponseType", 4, NScheme::NTypeIds::Uint32, false, false)
            .AddColumn(39, "FinalProgressRecord", 5, NScheme::NTypeIds::String, false, false)
            .AddColumn(39, "GeneratedFillContext", 6, NScheme::NTypeIds::String, false, false)
            .AddColumnToKey(39, 1)
            .AddColumnToKey(39, 2)
            .AddColumnToKey(39, 3);
        database.Begin().Apply(*alter.Flush()).Commit();
        NKikimrTxDataShard::TGeneratedColumnFillContext context;
        context.SetBuildId(1);
        context.SetSeqNoGeneration(2);
        context.SetSeqNoRound(3);
        context.SetOwnerId(4);
        context.SetPathId(5);
        context.SetSchemaVersion(6);
        context.SetTargetColumnId(7);
        context.SetSnapshotStep(8);
        context.SetSnapshotTxId(9);
        context.SetProgramFingerprint("program");
        TBuildIndexScanManager manager;
        database.Begin();
        NIceDb::TNiceDb add(*database.operator->());
        manager.PersistAdd(add, 1, 2, 3, 2, &context);
        database.Commit();
        database.Replay(NTable::NTest::EPlay::Boot);
        database.ReadVer(TRowVersion::Max());
        NIceDb::TNiceDb load(*database.operator->());
        TBuildIndexScanManager rebooted;
        UNIT_ASSERT(rebooted.Load(load));
        TString error;
        UNIT_ASSERT_C(rebooted.ValidateGeneratedFill(context, error), error);
        for (int mutation = 0; mutation < 5; ++mutation) {
            auto forged = context;
            if (mutation == 0) forged.SetTargetColumnId(10);
            if (mutation == 1) forged.SetSnapshotStep(10);
            if (mutation == 2) forged.SetSeqNoGeneration(10);
            if (mutation == 3) forged.SetSeqNoRound(10);
            if (mutation == 4) forged.SetProgramFingerprint("other program");
            UNIT_ASSERT(!rebooted.ValidateGeneratedFill(forged, error));
        }
        database.Cleanup().Begin();
        NIceDb::TNiceDb remove(*database.operator->());
        rebooted.PersistRemove(remove, 1, 2, 3);
        database.Commit();
        UNIT_ASSERT(!rebooted.ValidateGeneratedFill(context, error));
        database.Replay(NTable::NTest::EPlay::Boot);
        database.ReadVer(TRowVersion::Max());
        NIceDb::TNiceDb afterRemove(*database.operator->());
        UNIT_ASSERT(rebooted.Load(afterRemove));
        UNIT_ASSERT(!rebooted.ValidateGeneratedFill(context, error));
        database.Cleanup();
    }

    Y_UNIT_TEST(GeneratedFillRejectsStaleAttempt) {
        TBuildIndexScanManager manager;
        NKikimrTxDataShard::TGeneratedColumnFillContext context;
        TString error;
        UNIT_ASSERT(!manager.ValidateGeneratedFill(context, error));
        UNIT_ASSERT(!error.empty());
        context.SetBuildId(1);
        context.SetSeqNoGeneration(2);
        context.SetSeqNoRound(3);
        context.SetOwnerId(4);
        context.SetPathId(5);
        context.SetSchemaVersion(6);
        context.SetTargetColumnId(7);
        context.SetSnapshotStep(8);
        context.SetSnapshotTxId(9);
        context.SetProgramFingerprint("program");
        error.clear();
        UNIT_ASSERT(!manager.ValidateGeneratedFill(context, error));
        UNIT_ASSERT(!error.empty());
    }
}
} // namespace NKikimr::NDataShard
