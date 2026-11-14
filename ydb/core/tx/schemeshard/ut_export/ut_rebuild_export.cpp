#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

#include <google/protobuf/text_format.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

#include <algorithm>
#include <functional>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

enum class EExportIndex {
    Global,
    Unique,
    Fulltext,
};

struct TIndexedExportTable {
    static constexpr ui64 SchemeShard = TTestTxConfig::SchemeShard;
    const TString Table = "/MyRoot/Table";
    const TString Index = Table + "/ByValue";
    const EExportIndex Kind;
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
    TMap<TString, TVector<TString>> IndexRows;
    TMap<TString, ui64> IndexRowCounts;

    explicit TIndexedExportTable(EExportIndex kind)
        : Kind(kind)
        , Env(Runtime, TTestEnvOptions().EnableCompactFulltextIndex(false).EnableIndexMaterialization(true))
    {
        Runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            Columns { Name: "payload" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        Env.TestWaitNotification(Runtime, TxId);

        for (ui32 key = 1; key <= 3; ++key) {
            const TString value = key == 1 ? "green apple" : key == 2 ? "red apple" : "yellow pear";
            const TString payload = TStringBuilder() << "payload-" << key;
            UploadRow(Runtime, Table, 0, {1}, {2, 3}, {TCell::Make(key)},
                {TCell(value.data(), value.size()), TCell(payload.data(), payload.size())});
        }

        const ui64 buildId = Start(false);
        Finish(buildId);
        const auto description = DescribePrivatePath(Runtime, Index);
        for (const auto& child : description.GetPathDescription().GetChildren()) {
            if (child.GetPathType() == NKikimrSchemeOp::EPathTypeTable) {
                const TString path = Index + "/" + child.GetName();
                UNIT_ASSERT_C(CountRows(Runtime, SchemeShard, path) > 0, path);
                IndexRows.emplace(child.GetName(), ReadShards(Runtime, SchemeShard, path));
                IndexRowCounts.emplace(child.GetName(), CountRows(Runtime, SchemeShard, path));
            }
        }
        UNIT_ASSERT(!IndexRows.empty());
    }

    Ydb::Table::TableIndex Request(bool rebuild) const {
        Ydb::Table::TableIndex index;
        index.set_name("ByValue");
        Ydb::Table::GlobalIndexSettings* settings = nullptr;
        switch (Kind) {
        case EExportIndex::Global:
            settings = index.mutable_global_index()->mutable_settings();
            break;
        case EExportIndex::Unique:
            settings = index.mutable_global_unique_index()->mutable_settings();
            break;
        case EExportIndex::Fulltext:
            settings = index.mutable_global_fulltext_plain_index()->mutable_settings();
            if (!rebuild) {
                auto* column = index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings()->add_columns();
                column->set_column("value");
                column->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
                column->mutable_analyzers()->set_use_filter_lowercase(true);
            }
            break;
        }
        if (!rebuild) {
            index.add_index_columns("value");
            index.add_data_columns("payload");
            settings->mutable_partitioning_settings()->set_min_partitions_count(1);
            settings->mutable_partitioning_settings()->set_max_partitions_count(4);
            settings->mutable_partitioning_settings()->set_partition_size_mb(64);
        }
        return index;
    }

    ui64 Start(bool rebuild = true) {
        return Start(Request(rebuild), rebuild);
    }

    ui64 Start(const Ydb::Table::TableIndex& index, bool rebuild = true) {
        const ui64 buildId = ++TxId;
        auto* request = CreateBuildIndexRequest(buildId, "/MyRoot", Table, index);
        request->Record.MutableSettings()->set_is_rebuild(rebuild);
        const auto sender = Runtime.AllocateEdgeActor();
        Runtime.SendToPipe(SchemeShard, sender, request);
        const auto response = Runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS,
            response->Get()->Record.DebugString());
        return buildId;
    }

    TString TemporaryIndex(ui64 buildId) const {
        return TStringBuilder() << Table << "/__ydb_rebuild_" << buildId;
    }

    void Finish(ui64 buildId) {
        Env.TestWaitNotification(Runtime, buildId);
        const auto result = TestGetBuildIndex(Runtime, SchemeShard, "/MyRoot", buildId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            result.DebugString());
        TestForgetBuildIndex(Runtime, ++TxId, SchemeShard, "/MyRoot", buildId);
        TestDescribeResult(DescribePrivatePath(Runtime, TemporaryIndex(buildId)), {NLs::PathNotExist});
        CheckIndex(Table);
    }

    void CheckIndex(const TString& table, ui32 indexCount = 1) {
        TestDescribeResult(DescribePath(Runtime, table), {NLs::IndexesCount(indexCount)});
        const auto description = DescribePrivatePath(Runtime, table + "/ByValue");
        TestDescribeResult(description, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"value"}), NLs::IndexDataColumns({"payload"})});
        const auto expectedType = Kind == EExportIndex::Global ? NKikimrSchemeOp::EIndexTypeGlobal
            : Kind == EExportIndex::Unique ? NKikimrSchemeOp::EIndexTypeGlobalUnique
            : NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetTableIndex().GetType(), expectedType);
        if (!IndexRows.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().ChildrenSize(), IndexRows.size());
            for (const auto& [name, rows] : IndexRows) {
                UNIT_ASSERT_VALUES_EQUAL_C(ReadShards(Runtime, SchemeShard, table + "/ByValue/" + name), rows, name);
            }
        }
    }
};

TString ReadExportFile(const TFsPath& path) {
    UNIT_ASSERT_C(path.IsFile(), path);
    return TFileInput(path.GetPath()).ReadAll();
}

ui64 StartExport(TIndexedExportTable& table, const TFsPath& basePath, const TString& destination, bool materialized) {
    TestExport(table.Runtime, ++table.TxId, "/MyRoot", Sprintf(R"(
        ExportToFsSettings {
            base_path: "%s"
            include_index_data: %s
            items {
                source_path: "/MyRoot/Table"
                destination_path: "%s"
            }
        }
    )", basePath.GetPath().c_str(), materialized ? "true" : "false", destination.c_str()));
    return table.TxId;
}

void CheckExport(TIndexedExportTable& table, const TFsPath& basePath, const TString& destination, bool materialized,
    const std::function<void()>& beforeTransfer = {})
{
    const ui64 exportId = StartExport(table, basePath, destination, materialized);
    if (beforeTransfer) {
        beforeTransfer();
    }
    table.Env.TestWaitNotification(table.Runtime, exportId);
    const auto result = TestGetExport(table.Runtime, exportId, "/MyRoot");
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetResponse().GetEntry().GetProgress(),
        Ydb::Export::ExportProgress::PROGRESS_DONE, result.DebugString());

    const auto path = basePath / destination;
    UNIT_ASSERT_VALUES_EQUAL(ReadExportFile(path / "data_00.csv"),
        "1,\"green+apple\",\"payload-1\"\n"
        "2,\"red+apple\",\"payload-2\"\n"
        "3,\"yellow+pear\",\"payload-3\"\n");
    Ydb::Table::CreateTableRequest schema;
    const TString scheme = ReadExportFile(path / "scheme.pb");
    UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(scheme, &schema), scheme);
    UNIT_ASSERT_VALUES_EQUAL(schema.columns_size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(schema.columns(0).name(), "key");
    UNIT_ASSERT_VALUES_EQUAL(schema.columns(1).name(), "value");
    UNIT_ASSERT_VALUES_EQUAL(schema.columns(2).name(), "payload");
    UNIT_ASSERT_VALUES_EQUAL(schema.primary_key_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(schema.primary_key(0), "key");
    // Export the public definition matching the snapshot, including its analyzer.
    UNIT_ASSERT_VALUES_EQUAL_C(schema.indexes_size(), 1, scheme);
    const auto& index = schema.indexes(0);
    UNIT_ASSERT_VALUES_EQUAL(index.name(), "ByValue");
    UNIT_ASSERT_VALUES_EQUAL(index.index_columns_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(index.index_columns(0), "value");
    UNIT_ASSERT_VALUES_EQUAL(index.data_columns_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(index.data_columns(0), "payload");
    const Ydb::Table::GlobalIndexSettings* settings = nullptr;
    switch (table.Kind) {
    case EExportIndex::Global:
        UNIT_ASSERT(index.has_global_index());
        settings = &index.global_index().settings();
        break;
    case EExportIndex::Unique:
        UNIT_ASSERT(index.has_global_unique_index());
        settings = &index.global_unique_index().settings();
        break;
    case EExportIndex::Fulltext:
        UNIT_ASSERT(index.has_global_fulltext_plain_index());
        settings = &index.global_fulltext_plain_index().settings();
        UNIT_ASSERT_VALUES_EQUAL(index.global_fulltext_plain_index().fulltext_settings().SerializeAsString(),
            table.Request(false).global_fulltext_plain_index().fulltext_settings().SerializeAsString());
        break;
    }
    UNIT_ASSERT_VALUES_EQUAL(settings->partitioning_settings().min_partitions_count(), 1);
    UNIT_ASSERT_VALUES_EQUAL(settings->partitioning_settings().max_partitions_count(), 4);
    UNIT_ASSERT_VALUES_EQUAL(settings->partitioning_settings().partition_size_mb(), 64);

    const TString metadata = ReadExportFile(path / "metadata.json");
    UNIT_ASSERT_C(!metadata.Contains("__ydb_rebuild_"), metadata);
    TVector<TString> children;
    path.ListNames(children);
    for (const auto& name : children) {
        UNIT_ASSERT_C(!name.StartsWith("__ydb_rebuild_"), name);
    }
    if (materialized) {
        for (const auto& [name, rows] : table.IndexRows) {
            Y_UNUSED(rows);
            const auto indexPath = path / "ByValue" / name;
            Ydb::Table::CreateTableRequest indexSchema;
            const TString indexScheme = ReadExportFile(indexPath / "scheme.pb");
            UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(indexScheme, &indexSchema), indexScheme);
            const auto description = DescribePrivatePath(table.Runtime, table.Index + "/" + name);
            const auto& liveSchema = description.GetPathDescription().GetTable();
            UNIT_ASSERT_VALUES_EQUAL(indexSchema.columns_size(), liveSchema.ColumnsSize());
            UNIT_ASSERT_VALUES_EQUAL(indexSchema.primary_key_size(), liveSchema.KeyColumnNamesSize());
            for (int key = 0; key < indexSchema.primary_key_size(); ++key) {
                UNIT_ASSERT_VALUES_EQUAL(indexSchema.primary_key(key), liveSchema.GetKeyColumnNames(key));
            }
            const TString data = ReadExportFile(indexPath / "data_00.csv");
            UNIT_ASSERT_VALUES_EQUAL_C(std::count(data.begin(), data.end(), '\n'),
                table.IndexRowCounts.at(name), indexPath);
            for (ui32 key = 1; key <= 3; ++key) {
                UNIT_ASSERT_C(data.Contains(Sprintf("payload-%u", key)), data);
            }
        }
    }
}

void ExportDuringRepeatedRebuild(EExportIndex kind, bool materialized) {
    TTempDir tempDir;
    TIndexedExportTable table(kind);
    for (ui32 round = 1; round <= 2; ++round) {
        const auto live = DescribePrivatePath(table.Runtime, table.Index);
        const ui64 liveId = live.GetPathDescription().GetSelf().GetPathId();
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> secondary(table.Runtime);
        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexRequest> fulltext(table.Runtime);
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> replacement(table.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize()
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex
                && record.GetTransaction(0).GetInternal();
        });
        Y_DEFER {
            secondary.Stop().Unblock();
            fulltext.Stop().Unblock();
            replacement.Stop().Unblock();
        };
        const ui64 buildId = table.Start();
        table.Runtime.WaitFor("rebuild scan before export", [&] { return !secondary.empty() || !fulltext.empty(); });
        const auto state = TestGetBuildIndex(table.Runtime, table.SchemeShard, "/MyRoot", buildId);
        UNIT_ASSERT_VALUES_EQUAL(state.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
        TestDescribeResult(DescribePrivatePath(table.Runtime, table.TemporaryIndex(buildId)), {NLs::PathExist});

        CheckExport(table, tempDir.Path(), Sprintf("during-%u", round), materialized);
        UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId(), liveId);
        table.CheckIndex(table.Table, 2);

        secondary.Stop().Unblock();
        fulltext.Stop().Unblock();
        table.Runtime.WaitFor("Ready index before replacement", [&] { return !replacement.empty(); });
        TestDescribeResult(DescribePrivatePath(table.Runtime, table.TemporaryIndex(buildId)),
            {NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        CheckExport(table, tempDir.Path(), Sprintf("replacing-%u", round), materialized);
        UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId(), liveId);
        table.CheckIndex(table.Table, 2);
        replacement.Stop().Unblock();
        table.Finish(buildId);
        UNIT_ASSERT_C(DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId() != liveId,
            "Rebuild did not replace the public index");
        CheckExport(table, tempDir.Path(), Sprintf("after-%u", round), true);
        table.CheckIndex(table.Table);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardRebuildExportTests) {
    Y_UNIT_TEST_FLAG(ExportDuringRepeatedGlobalIndexRebuild, Materialized) {
        ExportDuringRepeatedRebuild(EExportIndex::Global, Materialized);
    }

    Y_UNIT_TEST_FLAG(ExportDuringRepeatedUniqueIndexRebuild, Materialized) {
        ExportDuringRepeatedRebuild(EExportIndex::Unique, Materialized);
    }

    Y_UNIT_TEST_FLAG(ExportDuringRepeatedFulltextIndexRebuild, Materialized) {
        ExportDuringRepeatedRebuild(EExportIndex::Fulltext, Materialized);
    }

    Y_UNIT_TEST(MaterializedExportFinishesAfterFulltextIndexReplacement) {
        TTempDir tempDir;
        TIndexedExportTable table(EExportIndex::Fulltext);
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "index-reader@builtin", NACLib::InheritNone);
        TestModifyACL(table.Runtime, ++table.TxId, "/MyRoot", "Table", acl.SerializeAsString(),
            "index-owner@builtin");
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        const auto source = DescribePath(table.Runtime, table.Table);
        const ui64 liveId = DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId();
        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexRequest> scan(table.Runtime);
        ui64 copyTxId = 0;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> copy(table.Runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.TransactionSize()
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                copyTxId = record.GetTxId();
            }
            return false;
        });
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> copied(table.Runtime, [&](const auto& ev) {
            return copyTxId && ev->Get()->Record.GetTxId() == copyTxId;
        });
        Y_DEFER {
            scan.Stop().Unblock();
            copied.Stop().Unblock();
        };
        auto index = table.Request(false);
        index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings()->mutable_columns(0)
            ->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        const ui64 buildId = table.Start(index);
        table.Runtime.WaitFor("fulltext rebuild scan before export", [&] { return !scan.empty(); });

        CheckExport(table, tempDir.Path(), "crossing-replacement", true, [&] {
            table.Runtime.WaitFor("export snapshot before index replacement", [&] { return !copied.empty(); });
            const auto snapshot = DescribePrivatePath(table.Runtime,
                TStringBuilder() << "/MyRoot/export-" << table.TxId << "/0");
            TestDescribeResult(snapshot, {NLs::PathExist});
            UNIT_ASSERT_VALUES_EQUAL(snapshot.GetPathDescription().GetSelf().GetOwner(),
                source.GetPathDescription().GetSelf().GetOwner());
            UNIT_ASSERT_VALUES_EQUAL(snapshot.GetPathDescription().GetSelf().GetACL(),
                source.GetPathDescription().GetSelf().GetACL());
            scan.Stop().Unblock();
            table.Env.TestWaitNotification(table.Runtime, buildId);
            const auto result = TestGetBuildIndex(table.Runtime, table.SchemeShard, "/MyRoot", buildId);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                result.DebugString());
            UNIT_ASSERT(DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId() != liveId);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(table.Runtime, table.SchemeShard, table.Index + "/indexImplTable"), 3);
            TestDescribeResult(DescribePrivatePath(table.Runtime, table.TemporaryIndex(buildId)), {NLs::PathNotExist});
            TestForgetBuildIndex(table.Runtime, ++table.TxId, table.SchemeShard, "/MyRoot", buildId);
            copied.Stop().Unblock();
        });
        Ydb::Scheme::ModifyPermissionsRequest permissions;
        const TString file = ReadExportFile(tempDir.Path() / "crossing-replacement" / "permissions.pb");
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(file, &permissions), file);
        UNIT_ASSERT_VALUES_EQUAL_C(permissions.actions_size(), 2, file);
        UNIT_ASSERT_VALUES_EQUAL_C(permissions.actions(0).change_owner(), "index-owner@builtin", file);
        const auto& grant = permissions.actions(1).grant();
        UNIT_ASSERT_VALUES_EQUAL_C(grant.subject(), "index-reader@builtin", file);
        UNIT_ASSERT_VALUES_EQUAL_C(grant.permission_names_size(), 1, file);
        UNIT_ASSERT_VALUES_EQUAL_C(grant.permission_names(0), "ydb.generic.use", file);
        Ydb::Scheme::ModifyPermissionsRequest implPermissions;
        const TString implFile = ReadExportFile(tempDir.Path() / "crossing-replacement" / "ByValue" / "indexImplTable" / "permissions.pb");
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(implFile, &implPermissions), implFile);
        UNIT_ASSERT_VALUES_EQUAL_C(implPermissions.actions_size(), 1, implFile);
        UNIT_ASSERT_VALUES_EQUAL_C(implPermissions.actions(0).change_owner(), "index-owner@builtin", implFile);
    }

    Y_UNIT_TEST(MaterializedExportAfterReadyRebuildAndTableDrop) {
        TTempDir tempDir;
        TIndexedExportTable table(EExportIndex::Global);
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> replacement(table.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize()
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex
                && record.GetTransaction(0).GetInternal();
        });
        ui64 copyTxId = 0;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> copy(table.Runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.TransactionSize()
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                copyTxId = record.GetTxId();
            }
            return false;
        });
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> copied(table.Runtime, [&](const auto& ev) {
            return copyTxId && ev->Get()->Record.GetTxId() == copyTxId;
        });
        Y_DEFER {
            replacement.Stop().Unblock();
            copied.Stop().Unblock();
        };
        const ui64 buildId = table.Start();
        table.Runtime.WaitFor("Ready rebuild before export snapshot", [&] { return !replacement.empty(); });
        const ui64 exportId = StartExport(table, tempDir.Path(), "dropped-source", true);
        table.Runtime.WaitFor("export snapshot before dropping source", [&] { return !copied.empty(); });
        // Consistent copies include all Ready indexes, even the private replacement.
        TestDescribeResult(DescribePrivatePath(table.Runtime,
            TStringBuilder() << "/MyRoot/export-" << exportId << "/0/__ydb_rebuild_" << buildId),
            {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        replacement.Stop().Unblock();
        table.Finish(buildId);
        TestDropTable(table.Runtime, ++table.TxId, "/MyRoot", "Table");
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        TestDescribeResult(DescribePath(table.Runtime, table.Table), {NLs::PathNotExist});
        copied.Stop().Unblock();
        table.Env.TestWaitNotification(table.Runtime, exportId);
        const auto result = TestGetExport(table.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResponse().GetEntry().GetProgress(),
            Ydb::Export::ExportProgress::PROGRESS_CANCELLED, result.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "cannot infer permissions");
    }

    Y_UNIT_TEST(SchemaChangesDuringGlobalIndexRebuild) {
        TIndexedExportTable table(EExportIndex::Global);
        const auto source = DescribePrivatePath(table.Runtime, table.Table, true, true);
        const ui64 liveId = DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId();
        const TString split = TStringBuilder() << "SourceTabletId: "
            << source.GetPathDescription().GetTablePartitions(0).GetDatashardId() << R"(
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 2 } } } }
        )";
        const TString alter = R"(
            Name: "Table"
            PartitionConfig { PartitioningPolicy { SizeToSplit: 33554432 } }
        )";

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> filling(table.Runtime);
        Y_DEFER { filling.Stop().Unblock(); };
        const ui64 buildId = table.Start();
        table.Runtime.WaitFor("rebuild scan before schema changes", [&] { return !filling.empty(); });
        TestCopyTable(table.Runtime, ++table.TxId, "/MyRoot", "Copy", table.Table, NKikimrScheme::StatusMultipleModifications);
        TestMoveTable(table.Runtime, ++table.TxId, table.Table, "/MyRoot/Moved", {NKikimrScheme::StatusMultipleModifications});
        TestMoveIndex(table.Runtime, ++table.TxId, table.Table, "ByValue", "Renamed", false,
            {NKikimrScheme::StatusMultipleModifications});
        TestSplitTable(table.Runtime, ++table.TxId, table.Table, split, {NKikimrScheme::StatusMultipleModifications});
        TestAlterTable(table.Runtime, ++table.TxId, "/MyRoot", alter, {NKikimrScheme::StatusMultipleModifications});
        TestDescribeResult(DescribePath(table.Runtime, "/MyRoot/Copy"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(table.Runtime, "/MyRoot/Moved"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(table.Runtime, table.Table + "/Renamed"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(table.Runtime, table.Table, true), {NLs::PartitionCount(1)});
        UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(table.Runtime, table.Index).GetPathDescription().GetSelf().GetPathId(), liveId);
        UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(table.Runtime, table.Table, true, true)
            .GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit(),
            source.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit());
        table.CheckIndex(table.Table, 2);

        filling.Stop().Unblock();
        table.Finish(buildId);
        TestAlterTable(table.Runtime, ++table.TxId, "/MyRoot", alter);
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(table.Runtime, table.Table, true, true)
            .GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit(), 33554432);
        TestSplitTable(table.Runtime, ++table.TxId, table.Table, split);
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        TestDescribeResult(DescribePath(table.Runtime, table.Table, true), {NLs::PartitionCount(2)});
        table.CheckIndex(table.Table);

        // A second rebuild also works after changing the source shard layout.
        table.Finish(table.Start());
        TestCopyTable(table.Runtime, ++table.TxId, "/MyRoot", "Copy", table.Table);
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        table.CheckIndex("/MyRoot/Copy");
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(table.Runtime, table.SchemeShard, "/MyRoot/Copy"),
            ReadShards(table.Runtime, table.SchemeShard, table.Table));
        TestMoveTable(table.Runtime, ++table.TxId, "/MyRoot/Copy", "/MyRoot/Moved");
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        TestDescribeResult(DescribePath(table.Runtime, "/MyRoot/Copy"), {NLs::PathNotExist});
        table.CheckIndex("/MyRoot/Moved");
        TestMoveIndex(table.Runtime, ++table.TxId, "/MyRoot/Moved", "ByValue", "Renamed", false);
        table.Env.TestWaitNotification(table.Runtime, table.TxId);
        TestDescribeResult(DescribePrivatePath(table.Runtime, "/MyRoot/Moved/ByValue"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(table.Runtime, "/MyRoot/Moved/Renamed"),
            {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady), NLs::IndexKeys({"value"})});
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(table.Runtime, table.SchemeShard, "/MyRoot/Moved/Renamed/indexImplTable"),
            table.IndexRows.at("indexImplTable"));
    }
}
