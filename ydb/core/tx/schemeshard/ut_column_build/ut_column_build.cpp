#include <ydb/core/metering/metering.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/index/column_build_test_hooks.h>
#include <ydb/core/tx/schemeshard/ut_helpers/column_build.h>
#include <util/generic/scope.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(ColumnBuildSettingsTest) {
    Y_UNIT_TEST(NullDefaultIsAnOrdinaryColumn) {
        using TColumn = TIndexBuildInfo::TColumnBuildInfo;
        Ydb::TypedValue null;
        null.mutable_type()->set_type_id(Ydb::Type::INT32);
        null.mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
        NKikimrIndexBuilder::TColumnBuildSetting setting;
        setting.SetColumnName("nullable");
        *setting.MutableColumnType() = null.type();
        *setting.mutable_default_from_literal() = null;
        setting.SetFamily("cold");
        TString error;
        TColumn parsed;
        UNIT_ASSERT_C(TColumn::TryFromProto(setting, parsed, error), error);
        for (const auto& column : {TColumn("nullable", null, false, "cold"), parsed,
                TColumn::FromPersistedProto(setting)}) {
            UNIT_ASSERT(!column.DefaultFromLiteral);
            UNIT_ASSERT(!column.NeedsBackfill());
            UNIT_ASSERT_VALUES_EQUAL(column.ColumnType.SerializeAsString(), null.type().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(column.FamilyName, "cold");
            NKikimrIndexBuilder::TColumnBuildSetting serialized;
            column.SerializeToProto(&serialized, "/MyRoot/Table");
            UNIT_ASSERT(!serialized.has_default_from_literal());
            UNIT_ASSERT(serialized.default_from_sequence().empty());
            NKikimrSchemeOp::TColumnDescription description;
            UNIT_ASSERT_C(column.FillColumnDescription(description, error), error);
            UNIT_ASSERT(!description.HasDefaultFromLiteral());
            UNIT_ASSERT(!description.HasDefaultFromSequence());
        }
        setting.SetNotNull(true);
        UNIT_ASSERT(!TColumn::TryFromProto(setting, parsed, error));
        setting.SetNotNull(false);
        setting.set_default_from_sequence("seq");
        UNIT_ASSERT(!TColumn::TryFromProto(setting, parsed, error));
        setting.clear_default_from_sequence();
        setting.ClearColumnType();
        const auto legacy = TColumn::FromPersistedProto(setting);
        UNIT_ASSERT_VALUES_EQUAL(legacy.ColumnType.SerializeAsString(), null.type().SerializeAsString());
        UNIT_ASSERT(!legacy.DefaultFromLiteral);
        UNIT_ASSERT(!legacy.NeedsBackfill());
        setting.MutableColumnType()->set_type_id(Ydb::Type::UTF8);
        UNIT_ASSERT(!TColumn::TryFromProto(setting, parsed, error));
    }

    Y_UNIT_TEST_FLAGS(NullDefaultsWithoutLiteralSupport, WithData, MultiShard) {
        NColumnBuildTest::TFixture f(WithData, MultiShard, false);
        const NColumnBuildTest::TOrder order{"n1", "n2", "n3"};
        auto settings = NColumnBuildTest::Settings(order);
        for (auto& column : *settings.mutable_column_build_operation()->mutable_column()) {
            *column.mutable_default_from_literal()->mutable_type() = column.GetColumnType();
            column.mutable_default_from_literal()->mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
        }
        size_t scans = 0, uploads = 0;
        auto scanObserver = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto&) { ++scans; });
        auto uploadObserver = f.Runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto&) { ++uploads; });
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(f.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable;
        });
        const ui64 id = ++f.TxId;
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", settings));
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        f.Runtime.WaitFor("schema proposal", [&] { return !blocker.empty(); }, TDuration::Minutes(5));
        NColumnBuildTest::Inspect(f.Runtime, id, order, false);
        RebootTablet(f.Runtime, TTestTxConfig::SchemeShard, f.Runtime.AllocateEdgeActor());
        NColumnBuildTest::Inspect(f.Runtime, id, order, false);
        blocker.Stop().Unblock();
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        UNIT_ASSERT_VALUES_EQUAL(scans, 0);
        UNIT_ASSERT_VALUES_EQUAL(uploads, 0);
        f.AssertSchema(order);
        f.AssertValues(order);
        NColumnBuildTest::Inspect(f.Runtime, id, order);
    }

    Y_UNIT_TEST(NullDefaultsCancellation) {
        NColumnBuildTest::TFixture f(true, true, false);
        const NColumnBuildTest::TOrder order{"n1", "n2", "n3"};
        auto settings = NColumnBuildTest::Settings(order);
        for (auto& column : *settings.mutable_column_build_operation()->mutable_column()) {
            *column.mutable_default_from_literal()->mutable_type() = column.GetColumnType();
            column.mutable_default_from_literal()->mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
        }
        bool paused = false;
        auto pausedState = TIndexBuildInfo::EState::Invalid;
        NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = [&](const TIndexBuildInfo& build) {
            pausedState = build.State;
            paused = true;
            return true;
        };
        Y_DEFER { NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {}; };
        const ui64 id = ++f.TxId;
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", settings));
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        f.Runtime.WaitFor("persisted Filling state", [&] { return paused; }, TDuration::Minutes(5));
        UNIT_ASSERT(pausedState == TIndexBuildInfo::EState::Filling);
        RebootTablet(f.Runtime, TTestTxConfig::SchemeShard, f.Runtime.AllocateEdgeActor());
        NColumnBuildTest::Inspect(f.Runtime, id, order, false);
        NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {};
        TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
        f.AssertSchema({});
        f.AssertOriginal();
        NColumnBuildTest::Inspect(f.Runtime, id, order);
        f.Retry(order);
    }

    Y_UNIT_TEST(ReusedMessageDropsPreviousDefault) {
        using TColumn = TIndexBuildInfo::TColumnBuildInfo;
        Ydb::TypedValue value;
        value.mutable_type()->set_type_id(Ydb::Type::INT32);
        value.mutable_value()->set_int32_value(0);
        TColumn literal("literal", value, false, "");
        TColumn ordinary("ordinary", value.type(), false, "");
        NKikimrIndexBuilder::TColumnBuildSetting setting;
        literal.SerializeToProto(&setting, "/MyRoot/Table");
        ordinary.SerializeToProto(&setting, "/MyRoot/Table");
        UNIT_ASSERT(!setting.has_default_from_literal());
        UNIT_ASSERT(setting.default_from_sequence().empty());
    }
    Y_UNIT_TEST(SequenceBackfillWithoutLiteralSupport) {
        NColumnBuildTest::TFixture f(true, true, false);
        auto settings = NColumnBuildTest::Settings({"n1", "n2"});
        auto* sequence = settings.mutable_column_build_operation()->add_column();
        sequence->SetColumnName("row_id");
        sequence->MutableColumnType()->set_type_id(Ydb::Type::UINT64);
        sequence->SetNotNull(true);
        sequence->set_default_from_sequence("seq");
        sequence->set_bit_reverse_sequence_value(true);
        size_t scans = 0;
        auto observer = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto& ev) {
            ++scans;
            const auto& columns = ev->Get()->Record.GetColumnBuildSettings();
            UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), 1);
            const auto& column = columns.column(0);
            UNIT_ASSERT_VALUES_EQUAL(column.GetColumnName(), "row_id");
            UNIT_ASSERT(!column.HasColumnType());
            UNIT_ASSERT_VALUES_EQUAL(ui32(column.default_from_literal().type().type_id()), ui32(Ydb::Type::UINT64));
            UNIT_ASSERT(!column.default_from_literal().has_value());
        });
        const ui64 id = ++f.TxId;
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", settings));
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        UNIT_ASSERT(scans > 0);
        f.AssertOriginal();
        const auto result = TestGetBuildIndex(f.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", id);
        const auto& restored = result.GetIndexBuild().GetSettings().column_build_operation();
        UNIT_ASSERT_VALUES_EQUAL(restored.column_size(), 3);
        UNIT_ASSERT(!restored.column(2).has_default_from_literal());
        UNIT_ASSERT_VALUES_EQUAL(ui32(restored.column(2).GetColumnType().type_id()), ui32(Ydb::Type::UINT64));
        UNIT_ASSERT_VALUES_EQUAL(restored.column(2).default_from_sequence(), "/MyRoot/Table/seq");
        THashSet<ui64> ids;
        for (const auto shard : f.Shards) {
            const auto data = ReadTable(f.Runtime, shard, "Table", {"key"}, {"key", "n1", "n2", "row_id"});
            const auto rows = NClient::TValue::Create(data)["Result"]["List"];
            for (size_t i = 0; i < rows.Size(); ++i) {
                UNIT_ASSERT(rows[i]["n1"].IsNull());
                UNIT_ASSERT(rows[i]["n2"].IsNull());
                const ui64 rowId = rows[i]["row_id"];
                UNIT_ASSERT(rowId != 0);
                UNIT_ASSERT(ids.insert(rowId).second);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 8);
        const auto description = DescribePath(f.Runtime, "/MyRoot/Table");
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT(!column.GetIsBuildInProgress());
            if (column.GetName() == "row_id") {
                UNIT_ASSERT(column.GetNotNull());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromSequence(), "seq");
                UNIT_ASSERT(!column.HasDefaultFromLiteral());
            }
        }
    }
    Y_UNIT_TEST(ParameterizedColumnDescription) {
        using TColumn = TIndexBuildInfo::TColumnBuildInfo;
        Ydb::Type decimal;
        decimal.mutable_decimal_type()->set_precision(22);
        decimal.mutable_decimal_type()->set_scale(9);
        TColumn column("parameterized", decimal, false, "cold");
        NKikimrSchemeOp::TColumnDescription description;
        TString error;
        UNIT_ASSERT_C(column.FillColumnDescription(description, error), error);
        UNIT_ASSERT(description.HasTypeInfo());
        UNIT_ASSERT(!description.HasDefaultFromLiteral());
        UNIT_ASSERT(!description.HasDefaultFromSequence());
        UNIT_ASSERT_VALUES_EQUAL(description.GetTypeInfo().GetDecimalPrecision(), 22);
        UNIT_ASSERT_VALUES_EQUAL(description.GetTypeInfo().GetDecimalScale(), 9);
    }
    Y_UNIT_TEST(ColumnSourcesRoundTrip) {
        using TColumn = TIndexBuildInfo::TColumnBuildInfo;
        Ydb::Type intType;
        intType.set_type_id(Ydb::Type::INT32);
        Ydb::TypedValue zero;
        *zero.mutable_type() = intType;
        zero.mutable_value()->set_int32_value(0);
        Ydb::TypedValue empty;
        empty.mutable_type()->set_type_id(Ydb::Type::UTF8);
        empty.mutable_value()->set_text_value("");
        Ydb::Type sequenceType;
        sequenceType.set_type_id(Ydb::Type::UINT64);
        Ydb::Type decimalType;
        decimalType.mutable_decimal_type()->set_precision(22);
        decimalType.mutable_decimal_type()->set_scale(9);

        const TVector<TColumn> columns{
            TColumn("ordinary", intType, false, "cold"),
            TColumn("zero", zero, true, "default"),
            TColumn("empty", empty, false, "cold"),
            TColumn(TColumn::FromSequenceTag{}, "sequence", sequenceType, "/MyRoot/Table/seq", true, true, ""),
            TColumn("decimal", decimalType, false, "")
        };
        NKikimrIndexBuilder::TColumnBuildSettings persisted;
        NKikimrIndexBuilder::TColumnBuildSettings scanned;
        for (const auto& column : columns) {
            auto* setting = persisted.add_column();
            column.SerializeToProto(setting, "/MyRoot/Table");
            UNIT_ASSERT(setting->HasColumnType());
            UNIT_ASSERT_VALUES_EQUAL(setting->has_default_from_literal(), bool(column.DefaultFromLiteral));
            TColumn restored;
            TString error;
            UNIT_ASSERT_C(TColumn::TryFromProto(*setting, restored, error), error);
            UNIT_ASSERT_VALUES_EQUAL(restored.ColumnName, column.ColumnName);
            UNIT_ASSERT_VALUES_EQUAL(restored.ColumnType.SerializeAsString(), column.ColumnType.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(restored.NotNull, column.NotNull);
            UNIT_ASSERT_VALUES_EQUAL(restored.FamilyName, column.FamilyName);
            UNIT_ASSERT_VALUES_EQUAL(restored.DefaultFromSequence, column.DefaultFromSequence);
            UNIT_ASSERT_VALUES_EQUAL(restored.BitReverseSequenceValue, column.BitReverseSequenceValue);
            UNIT_ASSERT_VALUES_EQUAL(bool(restored.DefaultFromLiteral), bool(column.DefaultFromLiteral));
            UNIT_ASSERT_VALUES_EQUAL(restored.NeedsBackfill(), column.ColumnName != "ordinary" && column.ColumnName != "decimal");
            if (restored.DefaultFromLiteral) {
                UNIT_ASSERT_VALUES_EQUAL(restored.DefaultFromLiteral->SerializeAsString(), column.DefaultFromLiteral->SerializeAsString());
            }
            if (column.NeedsBackfill()) {
                column.SerializeBackfillToProto(scanned.add_column(), "/MyRoot/Table");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(persisted.column_size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(scanned.column_size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(0).GetColumnName(), "zero");
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(0).default_from_literal().value().int32_value(), 0);
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(1).GetColumnName(), "empty");
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(1).default_from_literal().value().text_value(), "");
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(2).GetColumnName(), "sequence");
        UNIT_ASSERT_VALUES_EQUAL(scanned.column(2).default_from_sequence(), "/MyRoot/Table/seq");
        UNIT_ASSERT(!scanned.column(2).default_from_literal().has_value());
        UNIT_ASSERT_VALUES_EQUAL(ui32(scanned.column(2).default_from_literal().type().type_id()), ui32(Ydb::Type::UINT64));
        for (const auto& column : scanned.column()) {
            UNIT_ASSERT(!column.HasColumnType()); // DataShard keeps its existing backfill wire format.
        }
    }

    Y_UNIT_TEST(LegacyColumnSourcesAndInvalidTypes) {
        using TColumn = TIndexBuildInfo::TColumnBuildInfo;
        NKikimrIndexBuilder::TColumnBuildSetting setting;
        setting.SetColumnName("legacy");
        setting.mutable_default_from_literal()->mutable_type()->set_type_id(Ydb::Type::UINT64);
        setting.mutable_default_from_literal()->mutable_value()->set_uint64_value(0);
        TColumn restored;
        TString error;
        UNIT_ASSERT_C(TColumn::TryFromProto(setting, restored, error), error);
        UNIT_ASSERT(restored.DefaultFromLiteral);
        UNIT_ASSERT_VALUES_EQUAL(ui32(restored.ColumnType.type_id()), ui32(Ydb::Type::UINT64));
        setting.set_default_from_sequence("seq");
        UNIT_ASSERT(!TColumn::TryFromProto(setting, restored, error));
        setting.mutable_default_from_literal()->clear_value();
        UNIT_ASSERT_C(TColumn::TryFromProto(setting, restored, error), error);
        UNIT_ASSERT(!restored.DefaultFromLiteral);
        UNIT_ASSERT(restored.NeedsBackfill());
        UNIT_ASSERT_VALUES_EQUAL(ui32(restored.ColumnType.type_id()), ui32(Ydb::Type::UINT64));
        setting.clear_default_from_sequence();
        UNIT_ASSERT(!TColumn::TryFromProto(setting, restored, error));
        setting.clear_default_from_literal();
        setting.MutableColumnType()->set_type_id(Ydb::Type::INT32);
        UNIT_ASSERT_C(TColumn::TryFromProto(setting, restored, error), error);
        UNIT_ASSERT(!restored.NeedsBackfill());
        setting.mutable_default_from_literal()->mutable_type()->set_type_id(Ydb::Type::UTF8);
        setting.mutable_default_from_literal()->mutable_value()->set_text_value("");
        UNIT_ASSERT(!TColumn::TryFromProto(setting, restored, error));
    }

    Y_UNIT_TEST(PersistedSourcesRoundTrip) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true).EnableParameterizedDecimal(true));
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 100);

        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path("/MyRoot/Table");
        auto* columns = settings.mutable_column_build_operation();
        auto* ordinary = columns->add_column();
        ordinary->SetColumnName("ordinary");
        ordinary->MutableColumnType()->set_type_id(Ydb::Type::INT32);
        ordinary->SetFamily("default");
        auto* zero = columns->add_column();
        zero->SetColumnName("zero");
        zero->MutableColumnType()->set_type_id(Ydb::Type::INT64);
        *zero->mutable_default_from_literal()->mutable_type() = zero->GetColumnType();
        zero->mutable_default_from_literal()->mutable_value()->set_int64_value(0);
        zero->SetNotNull(true);
        auto* empty = columns->add_column();
        empty->SetColumnName("empty");
        empty->MutableColumnType()->set_type_id(Ydb::Type::UTF8);
        *empty->mutable_default_from_literal()->mutable_type() = empty->GetColumnType();
        empty->mutable_default_from_literal()->mutable_value()->set_text_value("");
        auto* sequence = columns->add_column();
        sequence->SetColumnName("sequence");
        sequence->MutableColumnType()->set_type_id(Ydb::Type::UINT64);
        sequence->set_default_from_sequence("seq");
        sequence->set_bit_reverse_sequence_value(true);
        auto* decimal = columns->add_column();
        decimal->SetColumnName("decimal");
        decimal->MutableColumnType()->mutable_decimal_type()->set_precision(22);
        decimal->MutableColumnType()->mutable_decimal_type()->set_scale(9);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable;
        });
        const auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
            new TEvIndexBuilder::TEvCreateRequest(101, "/MyRoot", settings));
        auto response = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        runtime.WaitFor("schema proposal", [&] { return !blocker.empty(); }, TDuration::Minutes(5));
        const auto before = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", 101).GetIndexBuild().GetSettings();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        const auto after = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", 101).GetIndexBuild().GetSettings();
        UNIT_ASSERT_VALUES_EQUAL(before.column_build_operation().SerializeAsString(), after.column_build_operation().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(after.column_build_operation().column_size(), 5);
        for (int i = 0; i < columns->column_size(); ++i) {
            const auto& expected = columns->column(i);
            const auto& actual = after.column_build_operation().column(i);
            UNIT_ASSERT_VALUES_EQUAL(actual.GetColumnName(), expected.GetColumnName());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetColumnType().SerializeAsString(), expected.GetColumnType().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetNotNull(), expected.GetNotNull());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetFamily(), expected.GetFamily());
            UNIT_ASSERT_VALUES_EQUAL(actual.has_default_from_literal(), expected.has_default_from_literal());
            UNIT_ASSERT_VALUES_EQUAL(actual.default_from_literal().SerializeAsString(), expected.default_from_literal().SerializeAsString());
        }
        UNIT_ASSERT_VALUES_EQUAL(after.column_build_operation().column(3).default_from_sequence(), "/MyRoot/Table/seq");
        UNIT_ASSERT(after.column_build_operation().column(3).bit_reverse_sequence_value());
        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, 101);
    }

    Y_UNIT_TEST(OrdinaryOnlyWithoutLiteralSupport) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(false));
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 100);
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path("/MyRoot/Table");
        for (const auto& [name, type] : TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>{
                {"number", Ydb::Type::INT32}, {"text", Ydb::Type::UTF8}}) {
            auto* column = settings.mutable_column_build_operation()->add_column();
            column->SetColumnName(name);
            column->MutableColumnType()->set_type_id(type);
        }
        size_t scans = 0;
        auto observer = runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto&) { ++scans; });
        const auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
            new TEvIndexBuilder::TEvCreateRequest(101, "/MyRoot", settings));
        auto response = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        env.TestWaitNotification(runtime, 101);
        UNIT_ASSERT_VALUES_EQUAL(scans, 0);
        const auto build = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", 101);
        UNIT_ASSERT_VALUES_EQUAL(build.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::CheckColumns("Table", {"key", "number", "text"}, {}, {"key"}, true),
            [](const auto& description) {
                for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
                    UNIT_ASSERT(!column.GetIsBuildInProgress());
                    UNIT_ASSERT(!column.HasDefaultFromLiteral());
                    UNIT_ASSERT(!column.HasDefaultFromSequence());
                }
            }
        });
    }
}

Y_UNIT_TEST_SUITE(ColumnBuildTest) {
    Y_UNIT_TEST(ValidDefaultValue) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(1111);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::SUCCESS);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_C(listing.EntriesSize() == 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_C(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(DoNotRestoreDeletedRows) {
        TTestBasicRuntime runtime(1, false);
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint64" }
              Columns { Name: "index"   Type: "Uint64" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint64 '%u ) ) ) )
                    (let row   '( '('index (Uint64 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        auto fnDeleteRow = [&] (ui64 tabletId, ui32 key, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint64 '%u ) ) ) )
                    (return (AsList (EraseRow '__user__%s key ) ))
                )
            )", key, table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool enabledCapture = true;
        TVector<TAutoPtr<IEventHandle>> delayedUpsertRows;
        auto grab = [&delayedUpsertRows, &enabledCapture](TAutoPtr<IEventHandle>& ev) -> auto {
            if (enabledCapture && ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvUploadRowsRequest::EventType) {
                delayedUpsertRows.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedUpsertRows](IEventHandle&) {
            return delayedUpsertRows.size() > 0;
        });

        runtime.SetObserverFunc(grab);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.DispatchEvents(opts);
        UNIT_ASSERT_C(delayedUpsertRows.size() > 0, "not captured several events");

        for (ui32 delta = 0; delta < 50; ++delta) {
            fnDeleteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, "Table");
        }

        for (const auto& ev: delayedUpsertRows) {
            runtime.Send(ev);
        }

        enabledCapture = false;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});

        for (ui32 delta = 50; delta < 101; ++delta) {
            UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::FakeHiveTablets + 6, "__user__Table", "key", 1 + delta));
        }

        for (ui32 delta = 0; delta < 50; ++delta) {
            UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::FakeHiveTablets + 6, "__user__Table", "key", 1 + delta));
        }
    }

    Y_UNIT_TEST(BaseCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TStringBuilder meteringMessages;
        auto grabMeteringMessage = [&meteringMessages](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->Type == NMetering::TEvMetering::TEvWriteMeteringJson::EventType) {
                auto *msg = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>();
                meteringMessages << msg->MeteringJson;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(grabMeteringMessage);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::SUCCESS);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});
/*
        const TString meteringData = R"({"usage":{"start":0,"quantity":179,"finish":0,"unit":"request_unit","type":"delta"},"tags":{},"id":"106-72075186233409549-2-101-1818-101-1818","cloud_id":"CLOUD_ID_VAL","source_wt":0,"source_id":"sless-docapi-ydb-ss","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.requests.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";

        UNIT_ASSERT_NO_DIFF(meteringMessages, meteringData + "\n");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        TestForgetBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        Y_ASSERT(listing.EntriesSize() == 0);
*/
    }

    Y_UNIT_TEST(Cancelling) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        TestCancelBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());

        blocker.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Rejecting) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

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
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, "abcd", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"})});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

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

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        blocker.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Locking_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexTx = ++txId;
        AsyncBuildColumn(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Drop the table BEFORE locking stage
        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", "Table");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathNotExist});

        blocker.Stop().Unblock();

        {
            TAutoPtr<IEventHandle> handle;
            TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL_C(event->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST, event->Record.DebugString());
        }
    }

    Y_UNIT_TEST(Locking_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state because Initiating not started
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to skip DroppingColumns state because AlterMainTable failed
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Lock operation does not know about DEFAULT columns, so reject will be from AlterMainTable
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(AlterMainTable_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        // Try to add column that already exists
        const TString columnName = "value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::BAD_REQUEST);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 0, listing.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(AlterMainTable_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating not started
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to skip DroppingColumns state because AlterMainTable failed
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Initiating_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnBuild) {
                auto& op = *modifyScheme.MutableInitiateColumnBuild();
                // set invalid table name to fail the operation
                op.SetTable("");
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(5),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Initiating_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnBuild;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(5),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Filling_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime, [&](auto& ev) {
            auto& record = ev->Get()->Record;
            record.SetTargetName("");
            return false;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> applyingBlocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();
        applyingBlocker.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was no Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Filling_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime, [&](auto&) {
            return true;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was no Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Applying_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                auto& op = *modifyScheme.MutableApplyIndexBuild();
                // set invalid snapshot tx id to fail the operation
                op.SetSnapshotTxId(ui64(InvalidTxId));
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state if Applying failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Applying_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Applying operation cannot be rejected because it have to finalize the index building
            // If it failed, Rejection_Applying will be skipped and the table will not be available
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        };

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Unlocking_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // First drop lock belongs to Unlocking stage, second to Rejection_Unlocking
        bool firstLock = true;
        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (firstLock && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock) {
                auto& op = *modifyScheme.MutableLockConfig();
                // set invalid name to fail the operation
                op.SetName("");
                firstLock = false;
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state because Applying was successful
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Unlocking_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Unlock operation does not know about DEFAULT columns, so no rejection will occur
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Cancellation_DroppingColumns_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> cancellationDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexId = ++txId;
        AsyncBuildColumn(runtime, buildIndexId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto request = CreateCancelBuildIndexRequest(++txId, "/MyRoot/ServerLessDB", buildIndexId);
        ForwardToTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor(), request);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);
        cancellationDetector.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was no Cancellation_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        {
            // If operation is cancelled, it does not matter if the flag is disabled or enabled
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Cancellation_Applying_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> cancellationDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexId = ++txId;
        AsyncBuildColumn(runtime, buildIndexId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto request = CreateCancelBuildIndexRequest(++txId, "/MyRoot/ServerLessDB", buildIndexId);
        ForwardToTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor(), request);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);
        cancellationDetector.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was no Cancellation_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        {
            // If operation is cancelled, it does not matter if the flag is disabled or enabled
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(DisabledAndEnabledFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(false));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});

        // Enable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(true);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }
}

namespace {
using namespace NColumnBuildTest;

void CheckCancelled(TFixture& f, ui64 id, const TOrder& order) {
    f.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
    f.AssertSchema({});
    f.AssertOriginal();
    Inspect(f.Runtime, id, order);
    f.Retry(order);
    TestDropTable(f.Runtime, ++f.TxId, "/MyRoot", "Table");
    f.Env.TestWaitNotification(f.Runtime, f.TxId);
}

void CheckCorrectness(EFamily family, bool withData, bool multiShard) {
    for (const auto& order : Orderings(family)) {
        TFixture f(withData, multiShard);
        TVector<TString> scanned;
        for (const auto& name : order) {
            if (name.StartsWith('d')) {
                scanned.push_back(name);
            }
        }
        size_t requests = 0, uploads = 0;
        auto uploadObserver = f.Runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto&) { ++uploads; });
        auto observer = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto& ev) {
            ++requests;
            const auto& columns = ev->Get()->Record.GetColumnBuildSettings();
            UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), scanned.size());
            for (size_t i = 0; i < scanned.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(columns.column(i).GetColumnName(), scanned[i]);
                UNIT_ASSERT(!columns.column(i).HasColumnType());
            }
        });
        const auto id = f.Start(order);
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        UNIT_ASSERT_VALUES_EQUAL(requests != 0, !scanned.empty());
        if (scanned.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(uploads, 0);
        }
        f.AssertSchema(order);
        f.AssertValues(order);
        Inspect(f.Runtime, id, order);
    }
}

void CheckPreparationCancellation(EFamily family, bool withData, bool multiShard) {
    for (const auto& order : Orderings(family)) {
        for (size_t prefix = 0; prefix <= order.size(); ++prefix) {
            TFixture f(withData, multiShard);
            const auto cancelSender = f.Runtime.AllocateEdgeActor();
            size_t queued = 0;
            NSchemeShard::TColumnBuildTestHooks::OnColumnPrepared = [&](const TActorContext& ctx, NSchemeShard::TIndexBuildId id, size_t processed) {
                if (processed == prefix && queued++ == 0) {
                    // Enqueue only. Delivery happens after the whole preparation transaction;
                    // no actor dispatch, schema transaction or per-column commit is nested here.
                    ctx.Send(new IEventHandle(ctx.SelfID, cancelSender, new TEvIndexBuilder::TEvCancelRequest(10000, "/MyRoot", ui64(id))));
                }
            };
            Y_DEFER { NSchemeShard::TColumnBuildTestHooks::OnColumnPrepared = {}; };
            const auto id = f.Start(order);
            NSchemeShard::TColumnBuildTestHooks::OnColumnPrepared = {};
            UNIT_ASSERT_VALUES_EQUAL(queued, 1);
            const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCancelResponse>(cancelSender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(queued, 1);
            CheckCancelled(f, id, order);
        }
    }
}

enum class ECancelBarrier { BeforeSchema, BeforeFill, PartialBatch, PartialShard, BeforeApplying, TooLate };

void CheckBarrierCancellation(EFamily family, bool withData, bool multiShard) {
    for (const auto& order : Orderings(family)) {
        const bool defaults = AnyOf(order, [](const TString& name) { return name.StartsWith('d'); });
        TVector<ECancelBarrier> barriers{ECancelBarrier::BeforeSchema, ECancelBarrier::BeforeFill,
            ECancelBarrier::BeforeApplying, ECancelBarrier::TooLate};
        if (defaults && withData) {
            barriers.push_back(ECancelBarrier::PartialBatch);
        }
        if (defaults && multiShard) {
            barriers.push_back(ECancelBarrier::PartialShard);
        }
        for (const auto barrier : barriers) {
            TFixture f(withData, multiShard);
            size_t batches = 0, done = 0;
            auto progress = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
                const auto& record = ev->Get()->Record;
                batches += record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta() > 0;
                done += record.GetStatus() == NKikimrIndexBuilder::DONE;
            });
            TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> schema(f.Runtime, [&](const auto& ev) {
                const auto type = ev->Get()->Record.GetTransaction(0).GetOperationType();
                return (barrier == ECancelBarrier::BeforeSchema && type == NKikimrSchemeOp::ESchemeOpAlterTable)
                    || (barrier == ECancelBarrier::BeforeFill && !defaults && type == NKikimrSchemeOp::ESchemeOpCreateColumnBuild)
                    || (barrier == ECancelBarrier::TooLate && type == NKikimrSchemeOp::ESchemeOpApplyIndexBuild);
            });
            bool paused = false;
            TIndexBuildInfo::EState pausedState = TIndexBuildInfo::EState::Invalid;
            NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = [&](const TIndexBuildInfo& build) {
                if (barrier != ECancelBarrier::BeforeApplying || (defaults &&
                    (build.Shards.empty() || !build.InProgressShards.empty() || !build.ToUploadShards.empty()))) {
                    return false;
                }
                paused = true;
                pausedState = build.State;
                return true;
            };
            Y_DEFER { NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {}; };
            TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(f.Runtime, [&](const auto& ev) {
                return (barrier == ECancelBarrier::BeforeFill && defaults)
                    || (barrier == ECancelBarrier::PartialShard && ev->Get()->Record.GetTabletId() == f.Shards.back());
            });
            TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(f.Runtime, [&](const auto&) {
                return barrier == ECancelBarrier::PartialBatch;
            });
            const auto id = f.Start(order);
            if (barrier == ECancelBarrier::PartialBatch) {
                f.Runtime.WaitFor("first upload", [&] { return !uploads.empty(); }, TDuration::Minutes(5));
                uploads.Unblock(1);
                f.Runtime.WaitFor("uploaded batch", [&] { return batches > 0 && !uploads.empty(); }, TDuration::Minutes(5));
            } else if (barrier == ECancelBarrier::PartialShard) {
                f.Runtime.WaitFor("completed shard", [&] { return done > 0 && !scans.empty(); }, TDuration::Minutes(5));
            } else {
                f.Runtime.WaitFor("cancellation barrier", [&] { return !schema.empty() || !scans.empty() || paused; }, TDuration::Minutes(5));
            }
            if (barrier != ECancelBarrier::BeforeSchema) {
                f.AssertSchema(order, true);
            }
            if (barrier == ECancelBarrier::BeforeApplying) {
                UNIT_ASSERT(paused && pausedState == TIndexBuildInfo::EState::Filling);
            }
            Inspect(f.Runtime, id, order, false);
            TestDropTable(f.Runtime, ++f.TxId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});
            NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {};
            TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id,
                {barrier == ECancelBarrier::TooLate ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::SUCCESS});
            schema.Stop().Unblock();
            scans.Stop().Unblock();
            uploads.Stop().Unblock();
            if (barrier == ECancelBarrier::TooLate) {
                f.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
                f.AssertSchema(order);
                f.AssertValues(order);
                Inspect(f.Runtime, id, order);
                TestDropTable(f.Runtime, ++f.TxId, "/MyRoot", "Table");
                f.Env.TestWaitNotification(f.Runtime, f.TxId);
            } else {
                CheckCancelled(f, id, order);
            }
        }
    }
}

void CheckInvalidColumns(EFamily family, bool withData, bool multiShard) {
    TFixture f(withData, multiShard);
    for (const auto& order : Orderings(family)) {
        for (size_t position = 0; position < order.size(); ++position) {
            for (size_t kind = 0; kind < 8; ++kind) {
                auto settings = Settings(order);
                auto& column = *settings.mutable_column_build_operation()->mutable_column(position);
                switch (kind) {
                case 0: column.SetColumnName("key"); break;
                case 1: column.SetColumnName(order[(position + 1) % order.size()]); break;
                case 2: column.MutableColumnType()->Clear(); break;
                case 3:
                    column.MutableColumnType()->Clear();
                    column.MutableColumnType()->mutable_decimal_type()->set_precision(22);
                    column.MutableColumnType()->mutable_decimal_type()->set_scale(23);
                    break;
                case 4:
                    column.mutable_default_from_literal()->mutable_type()->Clear();
                    column.mutable_default_from_literal()->mutable_value()->set_int32_value(42);
                    break;
                case 5:
                    column.set_default_from_sequence("seq");
                    *column.mutable_default_from_literal()->mutable_type() = column.GetColumnType();
                    column.mutable_default_from_literal()->mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
                    break;
                case 6:
                    column.SetNotNull(true);
                    column.clear_default_from_literal();
                    column.clear_default_from_sequence();
                    break;
                case 7:
                    column.SetNotNull(true);
                    *column.mutable_default_from_literal()->mutable_type() = column.GetColumnType();
                    column.mutable_default_from_literal()->mutable_value()->Clear();
                    column.mutable_default_from_literal()->mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
                    break;
                }
                const auto edge = f.Runtime.AllocateEdgeActor();
                ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
                    new TEvIndexBuilder::TEvCreateRequest(++f.TxId, "/MyRoot", settings));
                const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
                UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST,
                    "position=" << position << " invalid=" << kind << " " << response->Get()->Record.DebugString());
                f.AssertSchema({});
                f.AssertOriginal();
            }
        }
    }
}

void CheckPartialFailure(EFamily family, bool multiShard) {
    for (const auto& order : Orderings(family)) {
        TFixture f(true, multiShard);
        size_t batches = 0;
        auto progress = f.Runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta() && ++batches == 2) {
                record.SetStatus(NKikimrIndexBuilder::BUILD_ERROR);
                record.SetUploadStatus(Ydb::StatusIds::GENERIC_ERROR);
            }
        });
        const auto id = f.Start(order);
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
        UNIT_ASSERT(batches >= 2);
        progress.Remove();
        f.AssertSchema({});
        f.AssertOriginal();
        Inspect(f.Runtime, id, order);
        f.Retry(order);
    }
}
} // namespace

Y_UNIT_TEST_SUITE(ColumnBuildBatchTest) {
    Y_UNIT_TEST(OrdinaryCancellationFromFilling) {
        NColumnBuildTest::TFixture f(true, true);
        const NColumnBuildTest::TOrder order{"n1", "n2", "n3"};
        bool paused = false;
        TIndexBuildInfo::EState pausedState = TIndexBuildInfo::EState::Invalid;
        NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = [&](const TIndexBuildInfo& build) {
            paused = true;
            pausedState = build.State;
            return true;
        };
        Y_DEFER { NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {}; };
        const auto id = f.Start(order);
        f.Runtime.WaitFor("persisted Filling state", [&] { return paused; }, TDuration::Minutes(5));
        UNIT_ASSERT(paused);
        UNIT_ASSERT(pausedState == TIndexBuildInfo::EState::Filling);
        NSchemeShard::TColumnBuildTestHooks::BeforeFillingProgress = {};
        TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
        CheckCancelled(f, id, order);
    }

    Y_UNIT_TEST(EmptySequenceLeafIsRejected) {
        NColumnBuildTest::TFixture f(true, true);
        auto settings = NColumnBuildTest::Settings({"n1"});
        auto* sequence = settings.mutable_column_build_operation()->add_column();
        sequence->SetColumnName("row_id");
        sequence->MutableColumnType()->set_type_id(Ydb::Type::UINT64);
        sequence->set_default_from_sequence("/MyRoot/Table/");
        const auto edge = f.Runtime.AllocateEdgeActor();
        ForwardToTablet(f.Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(++f.TxId, "/MyRoot", settings));
        const auto response = f.Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST,
            response->Get()->Record.DebugString());
        f.AssertSchema({});
        f.AssertOriginal();
    }

    Y_UNIT_TEST_FLAGS(DropAfterCancellation, WithData, MultiShard) {
        NColumnBuildTest::TFixture f(WithData, MultiShard);
        const NColumnBuildTest::TOrder order{"n1", "d1", "n2", "d2"};
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(f.Runtime);
        const auto id = f.Start(order);
        f.Runtime.WaitFor("backfill before drop", [&] { return !scans.empty(); }, TDuration::Minutes(5));
        TestDropTable(f.Runtime, ++f.TxId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});
        TestCancelBuildIndex(f.Runtime, ++f.TxId, TTestTxConfig::SchemeShard, "/MyRoot", id);
        scans.Stop().Unblock();
        f.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
        f.AssertSchema({});
        f.AssertOriginal();
        NColumnBuildTest::Inspect(f.Runtime, id, order);
        TestDropTable(f.Runtime, ++f.TxId, "/MyRoot", "Table");
        f.Env.TestWaitNotification(f.Runtime, f.TxId);
        TestDescribeResult(DescribePath(f.Runtime, "/MyRoot/Table"), {NLs::PathNotExist});
        f.CreateTable(WithData, MultiShard);
        f.Retry(order);
    }
#define COLUMN_FAMILY_TESTS(Name, Family) \
    Y_UNIT_TEST_FLAGS(Name##Correctness, WithData, MultiShard) { CheckCorrectness(Family, WithData, MultiShard); } \
    Y_UNIT_TEST_FLAGS(Name##PreparationCancellation, WithData, MultiShard) { CheckPreparationCancellation(Family, WithData, MultiShard); } \
    Y_UNIT_TEST_FLAGS(Name##BarrierCancellation, WithData, MultiShard) { CheckBarrierCancellation(Family, WithData, MultiShard); } \
    Y_UNIT_TEST_FLAGS(Name##InvalidColumns, WithData, MultiShard) { CheckInvalidColumns(Family, WithData, MultiShard); }
    COLUMN_FAMILY_TESTS(Ordinary, EFamily::Ordinary)
    COLUMN_FAMILY_TESTS(Default, EFamily::Default)
    COLUMN_FAMILY_TESTS(Mixed, EFamily::Mixed)
#undef COLUMN_FAMILY_TESTS
    Y_UNIT_TEST_FLAG(DefaultPartialFailure, MultiShard) { CheckPartialFailure(EFamily::Default, MultiShard); }
    Y_UNIT_TEST_FLAG(MixedPartialFailure, MultiShard) { CheckPartialFailure(EFamily::Mixed, MultiShard); }
}

namespace {
struct TLegacyColumnRow {
    using TStorage = Schema::BuildColumnOperationSettings;
    ui64 Number = 0;
    TString Name;
    Ydb::TypedValue Literal;
    TString Sequence;

    template<class T> bool HaveValue() const {
        return !std::is_same_v<T, TStorage::ColumnTypeProto> && !std::is_same_v<T, TStorage::HasDefaultFromLiteral>;
    }
    template<class T> auto GetValue() const {
        if constexpr (std::is_same_v<T, TStorage::ColumnNo>) {
            return Number;
        } else if constexpr (std::is_same_v<T, TStorage::NotNull> || std::is_same_v<T, TStorage::BitReverseSequenceValue>) {
            return !Sequence.empty();
        } else if constexpr (std::is_same_v<T, TStorage::HasDefaultFromLiteral>) {
            return false; // This column did not exist in the legacy schema.
        } else if constexpr (std::is_same_v<T, TStorage::ColumnName>) {
            return Name;
        } else if constexpr (std::is_same_v<T, TStorage::DefaultFromLiteral>) {
            return TString(Literal.SerializeAsString());
        } else if constexpr (std::is_same_v<T, TStorage::DefaultFromSequence>) {
            return Sequence;
        } else if constexpr (std::is_same_v<T, TStorage::FamilyName>) {
            return TString("cold");
        } else {
            return TString();
        }
    }
    template<class T, class TValue> auto GetValueOrDefault(TValue fallback) const {
        return HaveValue<T>() ? GetValue<T>() : fallback;
    }
};
}

Y_UNIT_TEST_SUITE(ColumnBuildStorageCompatibilityTest) {
    Y_UNIT_TEST(LegacyTypeOnlyLiteralRemainsADefault) {
        TIndexBuildInfo build;
        TLegacyColumnRow row;
        row.Name = "missing_value";
        row.Literal.mutable_type()->set_type_id(Ydb::Type::JSON);
        build.AddBuildColumnInfo(row);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns.size(), 1);
        UNIT_ASSERT(build.BuildColumns[0].DefaultFromLiteral);
        UNIT_ASSERT(!build.BuildColumns[0].DefaultFromLiteral->has_value());
        UNIT_ASSERT(build.BuildColumns[0].NeedsBackfill());
        NKikimrIndexBuilder::TColumnBuildSetting setting;
        build.BuildColumns[0].SerializeToProto(&setting, "/MyRoot/Table");
        TIndexBuildInfo::TColumnBuildInfo requested;
        TString error;
        UNIT_ASSERT(!TIndexBuildInfo::TColumnBuildInfo::TryFromProto(setting, requested, error));
    }

    Y_UNIT_TEST(LegacyInvalidLiteralDoesNotTruncateRestoredColumns) {
        TIndexBuildInfo build;
        for (size_t i = 0; i < 3; ++i) {
            TLegacyColumnRow row;
            row.Number = i;
            row.Name = TStringBuilder() << "legacy" << i;
            row.Literal.mutable_type()->set_type_id(Ydb::Type::JSON);
            row.Literal.mutable_value()->set_text_value(i == 1 ? "not [json]" : "{}");
            build.AddBuildColumnInfo(row);
        }
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[1].DefaultFromLiteral->value().text_value(), "not [json]");
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[2].ColumnName, "legacy2");
    }

    Y_UNIT_TEST(LegacyRowsDeriveTypeAndDefaultPresence) {
        TIndexBuildInfo build;
        TLegacyColumnRow zero;
        zero.Name = "zero";
        zero.Literal.mutable_type()->set_type_id(Ydb::Type::INT64);
        zero.Literal.mutable_value()->set_int64_value(0);
        build.AddBuildColumnInfo(zero);
        TLegacyColumnRow empty;
        empty.Number = 1;
        empty.Name = "empty";
        empty.Literal.mutable_type()->set_type_id(Ydb::Type::UTF8);
        empty.Literal.mutable_value()->set_text_value("");
        build.AddBuildColumnInfo(empty);
        TLegacyColumnRow sequence;
        sequence.Number = 2;
        sequence.Name = "sequence";
        sequence.Sequence = "seq";
        sequence.Literal.mutable_type()->set_type_id(Ydb::Type::UINT64); // Legacy carrier, without a value.
        build.AddBuildColumnInfo(sequence);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[0].ColumnName, "zero");
        UNIT_ASSERT_VALUES_EQUAL(ui32(build.BuildColumns[0].ColumnType.type_id()), ui32(Ydb::Type::INT64));
        UNIT_ASSERT(build.BuildColumns[0].DefaultFromLiteral);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[0].DefaultFromLiteral->value().int64_value(), 0);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[1].ColumnName, "empty");
        UNIT_ASSERT_VALUES_EQUAL(ui32(build.BuildColumns[1].ColumnType.type_id()), ui32(Ydb::Type::UTF8));
        UNIT_ASSERT(build.BuildColumns[1].DefaultFromLiteral);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[1].DefaultFromLiteral->value().text_value(), "");
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[2].ColumnName, "sequence");
        UNIT_ASSERT_VALUES_EQUAL(ui32(build.BuildColumns[2].ColumnType.type_id()), ui32(Ydb::Type::UINT64));
        UNIT_ASSERT(!build.BuildColumns[2].DefaultFromLiteral);
        UNIT_ASSERT_VALUES_EQUAL(build.BuildColumns[2].DefaultFromSequence, "seq");
        UNIT_ASSERT(build.BuildColumns[2].BitReverseSequenceValue);
        for (const auto& column : build.BuildColumns) {
            UNIT_ASSERT(column.NeedsBackfill());
            UNIT_ASSERT_VALUES_EQUAL(column.FamilyName, "cold");
        }
    }
}
