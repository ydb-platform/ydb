#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_flow_proposals.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/value/value.h>

#include <google/protobuf/text_format.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

struct TExportEnv {
    TTestBasicRuntime Runtime;
    TTestEnv Env{Runtime};
    TTempDir Directory;
    TPortManager Ports;
    const ui16 Port = Ports.GetPort();
    TS3Mock S3{{}, TS3Mock::TSettings(Port)};
    const bool IsFs;
    ui64 TxId = 100;

    explicit TExportEnv(bool isFs)
        : IsFs(isFs)
    {
        Runtime.GetAppData().FeatureFlags.SetEnableExportAutoDropping(false);
        if (IsFs) {
            Runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        } else {
            UNIT_ASSERT(S3.Start());
        }
    }

    void CreateTable(bool isColumn = false, bool indexed = false) {
        if (isColumn) {
            Runtime.GetAppData().FeatureFlags.SetEnableColumnTablesBackup(true);
            TestCreateColumnTable(Runtime, ++TxId, "/MyRoot", R"(
                Name: "Table"
                ColumnShardCount: 1
                Schema {
                    Columns { Name: "key" Type: "Uint32" NotNull: true }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: "key"
                }
            )");
        } else if (indexed) {
            Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
            TestCreateIndexedTable(Runtime, ++TxId, "/MyRoot", R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                }
                IndexDescription { Name: "index" KeyColumnNames: ["value"] }
            )");
        } else {
            TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
        }
        Env.TestWaitNotification(Runtime, TxId);
        if (isColumn) {
            const auto description = DescribePath(Runtime, "/MyRoot/Table");
            const ui64 pathId = description.GetPathId();
            const ui64 shardId = description.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards(0);
            auto sender = Runtime.AllocateEdgeActor();

            const std::vector<NArrow::NTest::TTestColumn> schema = {
                NArrow::NTest::TTestColumn("key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)).SetNullable(false),
                NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
            };
            const auto data = NTxUT::MakeTestBlob({0, 100}, schema, {}, {"key"});

            ui64 writeId = 0;
            std::vector<ui64> writeIds;
            ++TxId;

            NTxUT::WriteData(Runtime, sender, shardId, ++writeId, pathId, data, schema, &writeIds, NEvWrite::EModificationType::Upsert, TxId);
            const auto step = NTxUT::ProposeCommit(Runtime, sender, shardId, TxId, writeIds, TxId);
            NTxUT::PlanCommit(Runtime, sender, shardId, step, {TxId});
        } else {
            WriteRow(Runtime, ++TxId, "/MyRoot/Table", 0, 1, "before");
        }
    }

    void CreateChangefeed() {
        Runtime.GetAppData().FeatureFlags.SetEnableChangefeedsExport(true);
        TestCreateCdcStream(Runtime, ++TxId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
                Name: "feed"
                Mode: ECdcStreamModeUpdate
                Format: ECdcStreamFormatJson
                State: ECdcStreamStateReady
            }
        )");
        Env.TestWaitNotification(Runtime, TxId);
    }

    ui64 Export(const TString& settings = "", const TVector<TString>& extraTables = {}) {
        TStringBuilder extraItems;
        for (const auto& name : extraTables) {
            extraItems << "items { source_path: \"/MyRoot/" << name << "\" "
                << (IsFs ? "destination_path" : "destination_prefix") << ": \"" << name << "\" }\n";
        }

        const TString request = IsFs
            ? Sprintf(R"(
                ExportToFsSettings {
                    base_path: "%s"
                    items { source_path: "/MyRoot/Table" destination_path: "backup" }
                    %s %s
                }
            )", Directory.Path().c_str(), settings.c_str(), extraItems.c_str())
            : Sprintf(R"(
                ExportToS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { source_path: "/MyRoot/Table" destination_prefix: "backup" }
                    %s %s
                }
            )", Port, settings.c_str(), extraItems.c_str());
        TestExport(Runtime, ++TxId, "/MyRoot", request);
        return TxId;
    }

    TString ReadFile(const TString& name) {
        if (IsFs) {
            return TFileInput(Directory.Path() / "backup" / name).ReadAll();
        }

        const auto& data = S3.GetData();
        const auto* content = data.FindPtr("/backup/" + name);
        UNIT_ASSERT_C(content, "Missing backup file: " << name);
        return *content;
    }

    bool HasFile(const TString& name) {
        return IsFs ? (Directory.Path() / "backup" / name).Exists() : S3.GetData().contains("/backup/" + name);
    }

    ui64 SnapshotCount() {
        const auto result = LocalMiniKQL(Runtime, TTestTxConfig::SchemeShard, R"(
            (
                (let range '(
                    '('OwnerPathId (Null) (Void))
                    '('LocalPathId (Null) (Void))
                ))
                (let fields '('OwnerPathId))
                (return (AsList
                    (SetResult 'Result (SelectRange 'TableSchemeSnapshots range fields '()))
                ))
            )
        )");
        return NClient::TValue::Create(result)[0]["List"].Size();
    }

    void Forget(ui64 exportId) {
        TestForgetExport(Runtime, ++TxId, "/MyRoot", exportId);
        Env.TestWaitNotification(Runtime, exportId);

        TestGetExport(Runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(SnapshotCount(), 0);
    }
};

struct TDelayedCopy {
    TExportEnv& Test;
    ui64 CopyTxId = 0;
    TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> Proposal;
    TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> Completion;

    explicit TDelayedCopy(TExportEnv& test)
        : Test(test)
        , Proposal(test.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables;
        })
        , Completion(test.Runtime, [this](const auto& ev) {
            return CopyTxId && ev->Get()->Record.GetTxId() == CopyTxId;
        })
    {}

    void WaitForCopy() {
        Test.Runtime.WaitFor("consistent copy proposal", [&] { return !Proposal.empty(); });
        CopyTxId = Proposal.front()->Get()->Record.GetTxId();
        Proposal.Stop().Unblock();
        Test.Runtime.WaitFor("consistent copy completion", [&] { return !Completion.empty(); });
    }

    void Resume(bool reboot) {
        if (reboot) {
            Completion.clear();
            RebootTablet(Test.Runtime, TTestTxConfig::SchemeShard, Test.Runtime.AllocateEdgeActor());
            Test.Runtime.WaitFor("consistent copy completion after reboot", [&] { return !Completion.empty(); });
        }
        Completion.Stop().Unblock();
    }
};

void SchemeMatchesDataSnapshot(bool isFs, bool reboot, bool isColumn, bool dropSource) {
    TExportEnv t(isFs);
    t.CreateTable(isColumn);
    TDelayedCopy copy(t);
    const ui64 exportId = t.Export();
    copy.WaitForCopy();
    UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);

    if (dropSource) {
        if (isColumn) {
            TestDropColumnTable(t.Runtime, ++t.TxId, "/MyRoot", "Table");
        } else {
            TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", "Table");
        }
    } else if (isColumn) {
        TestAlterColumnTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Table"
            AlterSchema { AddColumns { Name: "extra" Type: "Utf8" } }
        )");
    } else {
        WriteRow(t.Runtime, ++t.TxId, "/MyRoot/Table", 0, 1, "after");
        TestAlterTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Utf8" }
        )");
    }
    t.Env.TestWaitNotification(t.Runtime, t.TxId);

    copy.Resume(reboot);
    t.Env.TestWaitNotification(t.Runtime, exportId);
    TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);

    Ydb::Table::CreateTableRequest scheme;
    const TString schemeText = t.ReadFile("scheme.pb");
    UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeText, &scheme), schemeText);

    UNIT_ASSERT_VALUES_EQUAL(scheme.columns_size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(scheme.columns(0).name(), "key");
    UNIT_ASSERT_VALUES_EQUAL(scheme.columns(1).name(), "value");

    if (!isColumn) {
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
    } else {
        UNIT_ASSERT(!t.ReadFile("data_00.csv").empty());
    }

    t.Forget(exportId);
    RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardExportTableSchemeSnapshotTests) {
    Y_UNIT_TEST(BackupCopiesOutsideExportDoNotCaptureScheme) {
        TExportEnv t(false);
        t.CreateTable();
        TestConsistentCopyTables(t.Runtime, ++t.TxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/Backup"
                IsBackup: true
                OmitIndexes: true
            }
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
    }

    Y_UNIT_TEST(RegularCopyCanCaptureTableSchemeSnapshot) {
        TExportEnv t(false);
        t.CreateTable();
        TestConsistentCopyTables(t.Runtime, ++t.TxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/Copy"
                CaptureTableSchemeSnapshot: true
                OmitIndexes: true
            }
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);

        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);

        TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", "Copy");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
    }

    Y_UNIT_TEST(OrphanTableSchemeSnapshotIsRemovedAfterReboot) {
        TExportEnv t(false);
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '999999))
                ))
                (let name '('TableName (Utf8 '"orphan")))
                (let description '('TableDescription (String '"")))
                (let topics '('ChangefeedUnderlyingTopics (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(name description topics))))
            )
        )", TTestTxConfig::SchemeShard));
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
    }

    Y_UNIT_TEST_QUAD(AlterSourceAfterCopy, IsFs, Reboot) {
        SchemeMatchesDataSnapshot(IsFs, Reboot, false, false);
    }

    Y_UNIT_TEST_QUAD(DropSourceAfterCopy, IsFs, Reboot) {
        SchemeMatchesDataSnapshot(IsFs, Reboot, false, true);
    }

    Y_UNIT_TEST_TWIN(AlterColumnSourceAfterCopy, Reboot) {
        SchemeMatchesDataSnapshot(false, Reboot, true, false);
    }

    Y_UNIT_TEST_TWIN(DropColumnSourceAfterCopy, Reboot) {
        SchemeMatchesDataSnapshot(false, Reboot, true, true);
    }

    Y_UNIT_TEST_TWIN(LegacyExportWithoutTableSchemeSnapshot, IsFs) {
        TExportEnv t(IsFs);
        t.CreateTable();
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export();
        copy.WaitForCopy();

        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '%lu))
                ))
                (return (AsList (EraseRow 'TableSchemeSnapshots key)))
            )
        )", TTestTxConfig::SchemeShard, pathId));
        copy.Resume(true);
        t.Env.TestWaitNotification(t.Runtime, exportId);

        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
        UNIT_ASSERT(!t.ReadFile("scheme.pb").empty());
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(InvalidTableSchemeSnapshotCancelsWholeExport, IsFs) {
        TExportEnv t(IsFs);
        t.CreateTable();
        TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Other"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("", {"Other"});
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 2);
        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();

        // A valid protobuf without a table must fail preparation after recovery
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '%lu))
                ))
                (let description '('TableDescription (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(description))))
            )
        )", TTestTxConfig::SchemeShard, pathId));
        copy.Resume(true);
        t.Env.TestWaitNotification(t.Runtime, exportId);

        auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = result.GetResponse().GetEntry();
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "snapshot");
        UNIT_ASSERT(!t.HasFile("scheme.pb"));
        UNIT_ASSERT(!t.HasFile("export_complete.json"));

        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());

        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(DroppedSourceBeforeCaptureCancelsExport, IsFs) {
        TExportEnv t(IsFs);
        t.CreateTable();
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export();
        t.Runtime.WaitFor("consistent copy proposal", [&] { return !copy.Proposal.empty(); });

        TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", "Table");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        copy.Proposal.Stop().Unblock();
        t.Env.TestWaitNotification(t.Runtime, exportId);

        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
        UNIT_ASSERT(!t.HasFile("scheme.pb"));
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(IndexesAndChangefeedsSurviveSourceDrop, IsFs) {
        TExportEnv t(IsFs);
        t.CreateTable(false, true);
        t.CreateChangefeed();
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 2);
        TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", "Table");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);

        copy.Resume(true);
        t.Env.TestWaitNotification(t.Runtime, exportId);

        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));

        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");

        UNIT_ASSERT(!t.ReadFile("feed/changefeed_description.pb").empty());
        UNIT_ASSERT(!t.ReadFile("feed/topic_description.pb").empty());

        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("index/indexImplTable/scheme.pb"), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.primary_key_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(scheme.primary_key(0), "value");
        UNIT_ASSERT_VALUES_EQUAL(scheme.primary_key(1), "key");
        t.Forget(exportId);
    }

    Y_UNIT_TEST(EncryptedIndexNamesUseTableSchemeSnapshot) {
        NKikimrSchemeOp::TBackupTask snapshot;
        auto& table = *snapshot.MutableTable()->MutableTable();
        auto& index = *table.AddTableIndexes();
        index.SetName("index");
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        index.AddKeyColumnNames("value");

        NKikimrSchemeOp::TPathDescription changedSource(snapshot.GetTable());
        changedSource.MutableTable()->AddCdcStreams()->SetName("new-feed");

        UNIT_ASSERT_VALUES_EQUAL(NSchemeShard::ComputeIndexItemSuffix(&snapshot, changedSource, "index/indexImplTable", true), "001");
        UNIT_ASSERT_VALUES_EQUAL(NSchemeShard::ComputeIndexItemSuffix(nullptr, changedSource, "index/indexImplTable", true), "002");
        UNIT_ASSERT_VALUES_EQUAL(NSchemeShard::ComputeIndexItemSuffix(&snapshot, changedSource, "index/indexImplTable", false), "index/indexImplTable");
    }

    Y_UNIT_TEST_TWIN(SchemeFailureWaitsForProposedSibling, RejectProposal) {
        TExportEnv t(false);
        t.CreateTable();
        for (const TString name : {"Other", "Another"}) {
            TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                Name: "%s"
                Columns { Name: "key" Type: "Uint32" }
                KeyColumnNames: ["key"]
            )", name.c_str()));
            t.Env.TestWaitNotification(t.Runtime, t.TxId);
        }

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("", {"Other", "Another"});
        copy.WaitForCopy();

        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/2", exportId)).GetPathId();
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '%lu))
                ))
                (let description '('TableDescription (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(description))))
            )
        )", TTestTxConfig::SchemeShard, pathId));

        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> allocations(t.Runtime, [exportId](const auto& ev) {
            return ev->Cookie == exportId;
        });

        ui64 firstBackupTxId = 0;
        TBlockEvents<TEvDataShard::TEvSchemaChanged> schemaChanged(t.Runtime, [&](const auto& ev) {
            return firstBackupTxId && ev->Get()->Record.GetTxId() == firstBackupTxId;
        });

        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletion> subscriptions(t.Runtime, [&](const auto& ev) {
            return firstBackupTxId && ev->Get()->Record.GetTxId() == firstBackupTxId;
        });

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> secondProposal(t.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup() && record.GetTransaction(0).GetBackup().GetTableName() == "1";
        });

        copy.Resume(true);
        t.Runtime.WaitFor("three backup allocations", [&] { return allocations.size() == 3; });

        firstBackupTxId = allocations.front()->Get()->TxIds.front();
        allocations.Unblock(1);
        t.Runtime.WaitFor("first backup accepted", [&] { return !subscriptions.empty(); });
        allocations.Unblock(1);
        t.Runtime.WaitFor("second backup proposed", [&] { return !secondProposal.empty(); });
        const ui64 secondBackupTxId = secondProposal.front()->Get()->Record.GetTxId();

        TBlockEvents<TEvSchemeShard::TEvCancelTxResult> acknowledgements(t.Runtime);
        allocations.Stop().Unblock();
        t.Runtime.WaitFor("first backup cancellation", [&] { return !acknowledgements.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(acknowledgements.front()->Get()->Record.GetTargetTxId(), firstBackupTxId);
        acknowledgements.Unblock(1);

        const auto cancelling = TestGetExport(t.Runtime, exportId, "/MyRoot");
        const auto& entry = cancelling.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(!entry.HasEndTime());

        if (RejectProposal) {
            auto proposal = std::move(secondProposal.front());
            secondProposal.pop_front();
            secondProposal.Stop();

            auto rejected = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(NKikimrScheme::StatusSchemeError, secondBackupTxId, TTestTxConfig::SchemeShard);
            rejected->Record.SetReason("Injected proposal rejection");
            t.Runtime.Send(new IEventHandle(proposal->Sender, proposal->Recipient, rejected.Release()), 0, true);
        } else {
            secondProposal.Stop().Unblock();
            t.Runtime.WaitFor("second backup cancellation", [&] {
                for (const auto& ack : acknowledgements) {
                    if (ack->Get()->Record.GetTargetTxId() == secondBackupTxId) {
                        return true;
                    }
                }
                return false;
            });
        }

        acknowledgements.Stop().Unblock();
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(!t.HasFile("export_complete.json"));

        schemaChanged.Stop().Unblock();
        subscriptions.Stop().Unblock();
        t.Forget(exportId);
    }
}
