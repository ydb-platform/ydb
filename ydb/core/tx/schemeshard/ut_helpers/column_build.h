#pragma once

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/public/lib/value/value.h>

#include <algorithm>

namespace NSchemeShardUT_Private::NColumnBuildTest {

using TOrder = TVector<TString>;
enum class EFamily { Ordinary, Default, Mixed };

inline TVector<TOrder> Orderings(EFamily family) {
    if (family == EFamily::Ordinary) {
        return {{"n1", "n2"}, {"n2", "n1"}, {"n1", "n2", "n3"}};
    }
    if (family == EFamily::Default) {
        return {{"d1", "d2"}, {"d2", "d1"}, {"d1", "d2", "d3"}};
    }
    TVector<TOrder> result;
    TOrder order{"d1", "d2", "n1", "n2"};
    do {
        result.push_back(order);
    } while (std::next_permutation(order.begin(), order.end()));
    return result;
}

inline NKikimrIndexBuilder::TColumnBuildSetting Column(const TString& name) {
    NKikimrIndexBuilder::TColumnBuildSetting column;
    column.SetColumnName(name);
    if (name == "n1") {
        column.MutableColumnType()->set_type_id(Ydb::Type::INT32);
    } else if (name == "n2") {
        column.MutableColumnType()->set_type_id(Ydb::Type::UTF8);
        column.SetFamily("cold");
    } else if (name == "n3") {
        auto* decimal = column.MutableColumnType()->mutable_decimal_type();
        decimal->set_precision(22);
        decimal->set_scale(9);
    } else if (name == "d1") {
        column.MutableColumnType()->set_type_id(Ydb::Type::INT64);
        column.mutable_default_from_literal()->mutable_value()->set_int64_value(0);
    } else if (name == "d2") {
        column.MutableColumnType()->set_type_id(Ydb::Type::STRING);
        column.mutable_default_from_literal()->mutable_value()->set_bytes_value("");
        column.SetNotNull(true);
        column.SetFamily("cold");
    } else {
        UNIT_ASSERT_VALUES_EQUAL(name, "d3");
        column.MutableColumnType()->set_type_id(Ydb::Type::UINT64);
        column.mutable_default_from_literal()->mutable_value()->set_uint64_value(17);
    }
    if (column.has_default_from_literal()) {
        *column.mutable_default_from_literal()->mutable_type() = column.GetColumnType();
    }
    return column;
}

inline NKikimrIndexBuilder::TIndexBuildSettings Settings(const TOrder& order) {
    NKikimrIndexBuilder::TIndexBuildSettings result;
    result.set_source_path("/MyRoot/Table");
    result.MutableScanSettings()->SetMaxBatchRows(1);
    result.set_max_shards_in_flight(2);
    for (const auto& name : order) {
        *result.mutable_column_build_operation()->add_column() = Column(name);
    }
    return result;
}

// Runs assertions in the SchemeShard actor, after its final transaction commits.
class TInspectBuild final : public NSchemeShard::ISSDataProcessor {
public:
    ui64 BuildId = 0;
    bool Finished = true;
    bool Resume = false;

    TString GetClassName() const override { return "ColumnBuildTest.InspectBuild"; }
    TString DebugString() const override { return GetClassName(); }
    TString DoSerializeToString() const override { return ToString(BuildId) + ":" + ToString(Finished) + ":" + ToString(Resume); }
    bool DoDeserializeFromString(const TString& data) override {
        TStringBuf id, tail, finished, resume;
        if (!TStringBuf(data).TrySplit(':', id, tail) || !tail.TrySplit(':', finished, resume)) {
            return false;
        }
        return TryFromString(id, BuildId) && TryFromString(finished, Finished) && TryFromString(resume, Resume);
    }
    void DoProcess(NSchemeShard::TSchemeShard& ss, NKikimrScheme::TEvProcessingResponse& response) const override {
        NKikimrIndexBuilder::TIndexBuild inspection;
        const auto* entry = ss.IndexBuilds.FindPtr(NSchemeShard::TIndexBuildId(BuildId));
        if (!entry) {
            response.MutableError()->SetErrorMessage("Build was not restored");
            return;
        }
        const auto& build = **entry;
        build.SerializeToProto(&ss, inspection.MutableSettings()->mutable_column_build_operation());
        const auto check = [&](bool condition, const char* message) {
            if (!condition) {
                inspection.AddIssues()->set_message(message);
            }
        };
        check(!build.IsBroken, "Restored build is broken");
        if (Finished) {
            check(build.IsFinished(), "Build is still active");
            check(!ss.LockedPaths.contains(build.TablePathId), "Table lock was not removed");
            check(!ss.TablesWithSnapshots.contains(build.TablePathId), "Table snapshot was not removed");
            check(!ss.SnapshotTables.contains(build.InitiateTxId), "Snapshot tables were not removed");
            check(!ss.SnapshotsStepIds.contains(build.InitiateTxId), "Snapshot step was not removed");
            check(build.InProgressShards.empty(), "Build shards are still active");
            check(build.ToUploadShards.empty(), "Build uploads are still queued");
        }
        response.MutableContent()->SetData(inspection.SerializeAsString());
        if (Resume) {
            ss.Resume({NSchemeShard::TIndexBuildId(BuildId)}, TActivationContext::AsActorContext());
        }
    }

    static inline const TFactory::TRegistrator<TInspectBuild> Registrator{"ColumnBuildTest.InspectBuild"};
};

inline NKikimrIndexBuilder::TIndexBuild InspectBuild(TTestActorRuntime& runtime, ui64 buildId, bool finished = true, bool resume = false) {
    TInspectBuild request;
    request.BuildId = buildId;
    request.Finished = finished;
    request.Resume = resume;
    const auto edge = runtime.AllocateEdgeActor();
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, edge, new TEvSchemeShard::TEvProcessingRequest(request));
    const auto response = runtime.GrabEdgeEvent<TEvSchemeShard::TEvProcessingResponse>(edge);
    UNIT_ASSERT_C(response->Get()->Record.HasContent(), response->Get()->Record.DebugString());
    NKikimrIndexBuilder::TIndexBuild inspection;
    UNIT_ASSERT(inspection.ParseFromString(response->Get()->Record.GetContent().GetData()));
    UNIT_ASSERT_C(inspection.IssuesSize() == 0, inspection.DebugString());
    return inspection;
}

inline void ResumeBuild(TTestActorRuntime& runtime, ui64 buildId) {
    InspectBuild(runtime, buildId, false, true);
}

inline void Inspect(TTestActorRuntime& runtime, ui64 buildId, const TOrder& order, bool finished = true) {
    const auto inspection = InspectBuild(runtime, buildId, finished);
    const auto& actual = inspection.GetSettings().column_build_operation();
    UNIT_ASSERT_VALUES_EQUAL(actual.column_size(), order.size());
    for (size_t i = 0; i < order.size(); ++i) {
        const auto expected = Column(order[i]);
        const auto& column = actual.column(i);
        UNIT_ASSERT_VALUES_EQUAL(column.GetColumnName(), expected.GetColumnName());
        UNIT_ASSERT_VALUES_EQUAL(column.GetColumnType().SerializeAsString(), expected.GetColumnType().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(column.has_default_from_literal(), expected.has_default_from_literal());
        UNIT_ASSERT_VALUES_EQUAL(column.default_from_literal().SerializeAsString(), expected.default_from_literal().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetFamily(), expected.GetFamily());
        UNIT_ASSERT_VALUES_EQUAL(column.GetNotNull(), expected.GetNotNull());
    }
}

class TFixture {
public:
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
    TVector<ui64> Shards;
    TVector<TString> OriginalRows;

    TFixture(bool withData, bool multiShard, bool literalSupport = true)
        : Env(Runtime, TTestEnvOptions().EnableAddColumsWithDefaults(literalSupport).EnableParameterizedDecimal(true))
    {
        CreateTable(withData, multiShard);
    }

    void CreateTable(bool withData, bool multiShard) {
        Shards.clear();
        NKikimrSchemeOp::TTableDescription table;
        table.SetName("Table");
        auto* key = table.AddColumns();
        key->SetName("key");
        key->SetType("Uint32");
        auto* value = table.AddColumns();
        value->SetName("value");
        value->SetType("Utf8");
        table.AddKeyColumnNames("key");
        table.SetUniformPartitionsCount(multiShard ? 2 : 1);
        auto* main = table.MutablePartitionConfig()->AddColumnFamilies();
        main->SetId(0);
        main->SetName("default");
        main->MutableStorageConfig()->MutableSysLog();
        main->MutableStorageConfig()->MutableLog();
        auto* cold = table.MutablePartitionConfig()->AddColumnFamilies();
        cold->SetId(1);
        cold->SetName("cold");
        TestCreateTable(Runtime, ++TxId, "/MyRoot", table.DebugString());
        Env.TestWaitNotification(Runtime, TxId);
        const auto description = DescribePath(Runtime, "/MyRoot/Table", true);
        for (const auto& partition : description.GetPathDescription().GetTablePartitions()) {
            Shards.push_back(partition.GetDatashardId());
        }
        UNIT_ASSERT_VALUES_EQUAL(Shards.size(), multiShard ? 2 : 1);
        if (withData) {
            // More than one row per shard: cancellation/failure can follow a real uploaded batch.
            for (size_t shard = 0; shard < Shards.size(); ++shard) {
                for (ui32 offset = 0; offset < 4; ++offset) {
                    const ui32 keyValue = (shard ? 3000000000u : 1u) + offset;
                    NKikimrMiniKQL::TResult result;
                    TString error;
                    const auto status = LocalMiniKQL(Runtime, Shards[shard], Sprintf(R"((
                        (let key '('('key (Uint32 '%u))))
                        (let row '('('value (Utf8 'original))))
                        (return (AsList (UpdateRow '__user__Table key row)))
                    ))", keyValue), result, error);
                    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, error);
                }
            }
        }
        OriginalRows = ReadOriginal();
    }

    TVector<TString> ReadOriginal() {
        TVector<TString> result;
        for (const auto shard : Shards) {
            result.push_back(ReadTable(Runtime, shard, "Table", {"key"}, {"key", "value"}).SerializeAsString());
        }
        return result;
    }

    void AssertOriginal() { UNIT_ASSERT_VALUES_EQUAL(ReadOriginal(), OriginalRows); }

    ui64 Start(const TOrder& order) {
        const ui64 id = ++TxId;
        const auto edge = Runtime.AllocateEdgeActor();
        ForwardToTablet(Runtime, TTestTxConfig::SchemeShard, edge,
            new TEvIndexBuilder::TEvCreateRequest(id, "/MyRoot", Settings(order)));
        const auto response = Runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        return id;
    }

    void Wait(ui64 id, Ydb::Table::IndexBuildState::State state) {
        Env.TestWaitNotification(Runtime, id);
        const auto build = TestGetBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot", id);
        UNIT_ASSERT_VALUES_EQUAL_C(build.GetIndexBuild().GetState(), state, build.DebugString());
    }

    void AssertSchema(const TOrder& order, bool hidden = false) {
        const auto description = DescribePath(Runtime, "/MyRoot/Table");
        const auto& table = description.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL_C(table.ColumnsSize(), 2 + order.size(), description.DebugString());
        THashMap<TString, const NKikimrSchemeOp::TColumnDescription*> columns;
        for (const auto& column : table.GetColumns()) {
            columns.emplace(column.GetName(), &column);
        }
        UNIT_ASSERT(columns.contains("key") && columns.contains("value"));
        UNIT_ASSERT_VALUES_EQUAL(columns.at("key")->GetTypeId(), NScheme::NTypeIds::Uint32);
        UNIT_ASSERT_VALUES_EQUAL(columns.at("value")->GetTypeId(), NScheme::NTypeIds::Utf8);
        UNIT_ASSERT_VALUES_EQUAL(table.KeyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.GetKeyColumnNames(0), "key");
        for (const auto* name : {"key", "value"}) {
            const auto& original = *columns.at(name);
            UNIT_ASSERT(!original.GetIsBuildInProgress());
            UNIT_ASSERT(!original.GetNotNull());
            UNIT_ASSERT(!original.HasDefaultFromLiteral());
            UNIT_ASSERT(!original.HasDefaultFromSequence());
            UNIT_ASSERT_VALUES_EQUAL(original.GetFamily(), 0);
        }
        for (const auto& name : order) {
            UNIT_ASSERT_C(columns.contains(name), description.DebugString());
            const auto& actual = *columns.at(name);
            const auto expected = Column(name);
            NScheme::TTypeInfo type;
            TString typeMod, error;
            Ydb::StatusIds::StatusCode status;
            UNIT_ASSERT_C(ExtractColumnTypeInfo(type, typeMod, expected.GetColumnType(), status, error), error);
            const auto restoredType = NScheme::TypeInfoModFromProtoColumnType(actual.GetTypeId(),
                actual.HasTypeInfo() ? &actual.GetTypeInfo() : nullptr);
            UNIT_ASSERT(type == restoredType.TypeInfo);
            UNIT_ASSERT_VALUES_EQUAL(actual.GetNotNull(), expected.GetNotNull());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetFamily(), expected.GetFamily() == "cold" ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(actual.HasDefaultFromLiteral(), expected.has_default_from_literal());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetIsBuildInProgress(), hidden);
            if (expected.has_default_from_literal()) {
                UNIT_ASSERT_VALUES_EQUAL(actual.GetDefaultFromLiteral().SerializeAsString(), expected.default_from_literal().SerializeAsString());
            }
        }
    }

    void AssertValues(const TOrder& order) {
        TVector<TString> names{"key", "value"};
        names.insert(names.end(), order.begin(), order.end());
        for (const auto shard : Shards) {
            const auto result = ReadTable(Runtime, shard, "Table", {"key"}, names);
            const auto rows = NClient::TValue::Create(result)["Result"]["List"];
            for (size_t i = 0; i < rows.Size(); ++i) {
                for (const auto& name : order) {
                    const auto value = rows[i][name];
                    if (name.StartsWith('n')) {
                        UNIT_ASSERT_C(value.IsNull(), name);
                    } else if (name == "d1") {
                        UNIT_ASSERT(value.HaveValue());
                        UNIT_ASSERT_VALUES_EQUAL(i64(value), 0);
                    } else if (name == "d2") {
                        UNIT_ASSERT_VALUES_EQUAL(TString(value), "");
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(ui64(value), 17);
                    }
                }
            }
        }
        AssertOriginal();
    }

    void Retry(const TOrder& order) {
        const auto id = Start(order);
        Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        AssertSchema(order);
        AssertValues(order);
        Inspect(Runtime, id, order);
    }
};

} // namespace NSchemeShardUT_Private::NColumnBuildTest
