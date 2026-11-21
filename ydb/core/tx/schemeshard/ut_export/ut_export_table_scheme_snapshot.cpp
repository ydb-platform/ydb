#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/value/value.h>

#include <google/protobuf/text_format.h>

#include <utility>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/escape.h>
#include <util/string/printf.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

struct TExportEnv {
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    TTempDir Directory;
    TPortManager Ports;
    const ui16 Port = Ports.GetPort();
    TS3Mock S3{{}, TS3Mock::TSettings(Port)};
    const bool IsFs;
    ui64 TxId = 100;

    explicit TExportEnv(bool isFs, TTestEnv::TSchemeShardFactory ssFactory = &CreateFlatTxSchemeShard,
            const TTestEnvOptions& envOptions = {})
        : Env(Runtime, envOptions, std::move(ssFactory))
        , IsFs(isFs)
    {
        Runtime.GetAppData().FeatureFlags.SetEnableExportAutoDropping(false);
        if (IsFs) {
            Runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        } else {
            UNIT_ASSERT(S3.Start());
        }
    }

    void ConfigureBackupConcurrency(ui32 limit) {
        for (ui32 nodeIdx = 0; nodeIdx < Runtime.GetNodeCount(); ++nodeIdx) {
            const auto sender = Runtime.AllocateEdgeActor(nodeIdx);
            auto event = MakeHolder<NResourceBroker::TEvResourceBroker::TEvConfigure>();
            event->Merge = true;
            auto* queue = event->Record.AddQueues();
            queue->SetName("queue_backup");
            queue->MutableLimit()->SetCpu(limit);
            Runtime.Send(new IEventHandle( Runtime.GetLocalServiceId(NResourceBroker::MakeResourceBrokerID(), nodeIdx), sender, event.Release()), nodeIdx);
            TAutoPtr<IEventHandle> handle;
            const auto* reply = Runtime.GrabEdgeEventRethrow<
                NResourceBroker::TEvResourceBroker::TEvConfigureResult>(handle);
            UNIT_ASSERT(reply->Record.GetSuccess());
        }
    }

    static TString RowTableDescription(const TString& name = "Table") {
        return Sprintf(R"(
            Name: "%s"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )", name.c_str());
    }

    void CreateKeyTable(const TString& name) {
        TestCreateTable(Runtime, ++TxId, "/MyRoot", Sprintf(R"(
            Name: "%s"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )", name.c_str()));
        Env.TestWaitNotification(Runtime, TxId);
    }

    void WriteColumnRows() {
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
            TestCreateIndexedTable(Runtime, ++TxId, "/MyRoot", Sprintf(R"(
                TableDescription { %s }
                IndexDescription { Name: "index" KeyColumnNames: ["value"] }
            )", RowTableDescription().c_str()));
        } else {
            TestCreateTable(Runtime, ++TxId, "/MyRoot", RowTableDescription());
        }
        Env.TestWaitNotification(Runtime, TxId);
        if (isColumn) {
            WriteColumnRows();
        } else {
            WriteRow(Runtime, ++TxId, "/MyRoot/Table", 0, 1, "before");
        }
    }

    void CreateIndexedTable() {
        CreateTable(false, true);
    }

    void CreateColumnTable() {
        CreateTable(true);
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

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::SchemeShard, Runtime.AllocateEdgeActor());
    }

    TString ExportPath(ui64 exportId) const {
        return Sprintf("/MyRoot/export-%lu/0", exportId);
    }

    TVector<TString> ReadRows(const TString& path) {
        return ReadShards(Runtime, TTestTxConfig::SchemeShard, path);
    }

    Ydb::Table::CreateTableRequest ReadScheme(const TString& file = "scheme.pb", const TString& table = "backup") {
        const auto text = ReadFile(file, table);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(text, &scheme), text);
        return scheme;
    }

    auto GetExportEntry(ui64 exportId, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        return TestGetExport(Runtime, exportId, "/MyRoot", status).GetResponse().GetEntry();
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

    void Import(bool rebootBeforeRestore = false) {
        const TString request = IsFs
            ? Sprintf(R"(
                ImportFromFsSettings {
                    base_path: "%s"
                    items { source_path: "backup" destination_path: "/MyRoot/Restored" }
                    index_population_mode: INDEX_POPULATION_MODE_IMPORT
                }
            )", Directory.Path().c_str())
            : Sprintf(R"(
                ImportFromS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { source_prefix: "backup" destination_path: "/MyRoot/Restored" }
                    index_population_mode: INDEX_POPULATION_MODE_IMPORT
                }
            )", Port);

        const ui64 importId = TxId + 1;
        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> allocations(Runtime, [=](const auto& ev) {
            return rebootBeforeRestore && ev->Cookie == importId;
        });

        TestImport(Runtime, ++TxId, "/MyRoot", request);
        if (rebootBeforeRestore) {
            Runtime.WaitFor("import allocation", [&] { return !allocations.empty(); });
            allocations.clear();
            Reboot();
            allocations.Stop().Unblock();
        }
        Env.TestWaitNotification(Runtime, importId);
        TestGetImport(Runtime, importId, "/MyRoot");
    }

    TString ReadFile(const TString& name, const TString& table = "backup") {
        if (IsFs) {
            return TFileInput(Directory.Path() / table / name).ReadAll();
        }

        const auto& data = S3.GetData();
        const auto* content = data.FindPtr("/" + table + "/" + name);
        UNIT_ASSERT_C(content, "Missing backup file: " << name);
        return *content;
    }

    bool HasFile(const TString& name, const TString& table = "backup") {
        return IsFs ? (Directory.Path() / table / name).Exists()
            : S3.GetData().contains("/" + table + "/" + name);
    }

    bool HasExportCompletionMarker() {
        return !IsFs && AnyOf(S3.GetData(), [](const auto& object) {
            return object.first.EndsWith("/export_complete.json");
        });
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

void UpdateTableSchemeSnapshot(TExportEnv& test, ui64 pathId, const TString& field, const TString& value) {
    LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let key '(
                '('OwnerPathId (Uint64 '%lu))
                '('LocalPathId (Uint64 '%lu))
            ))
            (let value '('%s (String '"%s")))
            (return (AsList (UpdateRow 'TableSchemeSnapshots key '(value))))
        )
    )", TTestTxConfig::SchemeShard, pathId, field.c_str(), EscapeC(value).c_str()));
}

void CorruptTableSchemeSnapshot(TExportEnv& test, ui64 pathId, bool topics = false) {
    UpdateTableSchemeSnapshot(test, pathId, topics ? "ChangefeedUnderlyingTopics" : "TableDescription", "!");
}

NSchemeShard::TExportInfo::EState ReadExportItemState(TExportEnv& test, ui64 exportId, ui32 itemIdx) {
    const auto result = LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let range '(
                '('ExportId (Uint64 '%lu) (Uint64 '%lu))
                '('Index (Uint32 '%u) (Uint32 '%u))
            ))
            (return (AsList (SetResult 'Result (SelectRange 'ExportItems range '('State) '()))))
        )
    )", exportId, exportId, itemIdx, itemIdx));
    const auto rows = NClient::TValue::Create(result)[0]["List"];
    UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 1);
    return static_cast<NSchemeShard::TExportInfo::EState>(static_cast<ui8>(rows[0]["State"]));
}

TVector<ui64> ReadExportBackupTxIds(TExportEnv& test, ui64 exportId) {
    const auto result = LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let range '(
                '('ExportId (Uint64 '%lu) (Uint64 '%lu))
                '('Index (Null) (Void))
            ))
            (return (AsList (SetResult 'Result (SelectRange 'ExportItems range '('BackupTxId) '()))))
        )
    )", exportId, exportId));
    const auto rows = NClient::TValue::Create(result)[0]["List"];
    TVector<ui64> txIds;

    for (ui32 i = 0; i < rows.Size(); ++i) {
        txIds.push_back(static_cast<ui64>(rows[i]["BackupTxId"]));
    }

    return txIds;
}

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

    void WaitForCopy(bool captureTableSchemeSnapshot = true) {
        Test.Runtime.WaitFor("consistent copy proposal", [&] { return !Proposal.empty(); });
        CopyTxId = Proposal.front()->Get()->Record.GetTxId();
        if (!captureTableSchemeSnapshot) {
            // An old SchemeShard does not request scheme capture.
            auto& copies = *Proposal.front()->Get()->Record.MutableTransaction(0)
                ->MutableCreateConsistentCopyTables()->MutableCopyTableDescriptions();

            for (auto& copy : copies) {
                copy.ClearCaptureTableSchemeSnapshot();
            }
        }
        Proposal.Stop().Unblock();
        Test.Runtime.WaitFor("consistent copy completion", [&] { return !Completion.empty(); });
    }

    void Reboot() {
        Completion.clear();
        Test.Reboot();
        Test.Runtime.WaitFor("consistent copy completion after reboot", [&] { return !Completion.empty(); });
    }

    void Resume(bool reboot) {
        if (reboot) {
            Reboot();
        }
        Completion.Stop().Unblock();
    }
};

void RebootTableShards(TExportEnv& test, const TString& path) {
    const auto description = DescribePrivatePath(test.Runtime, path, true, true);
    const auto& partitions = description.GetPathDescription().GetTablePartitions();
    UNIT_ASSERT_C(!partitions.empty(), path);

    for (const auto& partition : partitions) {
        RebootTablet(test.Runtime, partition.GetDatashardId(), test.Runtime.AllocateEdgeActor());
    }
}

template <typename TEv>
THashSet<TString> GetObjectKeys(const TBlockEvents<TEv>& requests) {
    THashSet<TString> keys;

    for (const auto& request : requests) {
        keys.insert(TString(request->Get()->Request.GetKey()));
    }

    return keys;
}

template <typename TEv>
THashSet<ui64> GetCancellationTargets(const TBlockEvents<TEv>& requests) {
    THashSet<ui64> targets;

    for (const auto& request : requests) {
        targets.insert(request->Get()->Record.GetTargetTxId());
    }

    return targets;
}

struct TBackupReplies {
    TExportEnv& Test;
    THashSet<ui64> TxIds;
    TTestActorRuntime::TEventObserverHolder Proposals;
    TBlockEvents<TEvSchemeShard::TEvModifySchemeTransactionResult> Replies;

    explicit TBackupReplies(TExportEnv& test, bool blockReplies = true)
        : Test(test)
        , Proposals(test.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([this](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup()) {
                TxIds.insert(record.GetTxId());
            }
        }))
        , Replies(test.Runtime, [this, blockReplies](const auto& ev) {
            return blockReplies && TxIds.contains(ev->Get()->Record.GetTxId());
        })
    {}

    void CheckAccepted() {
        for (const auto& reply : Replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrScheme::StatusAccepted);
        }
    }

    void DiscardReplies() {
        Proposals.Remove();
        Replies.clear();
        Replies.Stop();
    }

    void CheckRecoveredTxIds(ui64 exportId) {
        const auto ids = ReadExportBackupTxIds(Test, exportId);
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 2);
        UNIT_ASSERT_VALUES_UNEQUAL(ids[0], ids[1]);
        UNIT_ASSERT(TxIds.contains(ids[0]));
        UNIT_ASSERT(TxIds.contains(ids[1]));
    }
};

struct TProposedBackups {
    TExportEnv& Test;
    ui64 FirstTxId = 0;
    TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> Allocations;
    TBlockEvents<TEvDataShard::TEvSchemaChanged> SchemaChanged;
    TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletion> Subscriptions;
    TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> SecondProposal;
    TBlockEvents<TEvSchemeShard::TEvCancelTxResult> Acknowledgements;

    TProposedBackups(TExportEnv& test, ui64 exportId)
        : Test(test)
        , Allocations(test.Runtime, [exportId](const auto& ev) { return ev->Cookie == exportId; })
        , SchemaChanged(test.Runtime, [this](const auto& ev) {
            return FirstTxId && ev->Get()->Record.GetTxId() == FirstTxId;
        })
        , Subscriptions(test.Runtime, [this](const auto& ev) {
            return FirstTxId && ev->Get()->Record.GetTxId() == FirstTxId;
        })
        , SecondProposal(test.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup()
                && record.GetTransaction(0).GetBackup().GetTableName() == "1";
        })
        , Acknowledgements(test.Runtime)
    {}

    void StartFirst() {
        Test.Runtime.WaitFor("three backup allocations", [&] { return Allocations.size() == 3; });
        FirstTxId = Allocations.front()->Get()->TxIds.front();
        Allocations.Unblock(1);
        Test.Runtime.WaitFor("first backup accepted", [&] { return !Subscriptions.empty(); });
    }

    ui64 StartSecond() {
        Allocations.Unblock(1);
        Test.Runtime.WaitFor("second backup proposed", [&] { return !SecondProposal.empty(); });
        return SecondProposal.front()->Get()->Record.GetTxId();
    }

    void FailThird() {
        Allocations.Stop().Unblock();
        Test.Runtime.WaitFor("first backup cancellation", [&] { return !Acknowledgements.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(Acknowledgements.front()->Get()->Record.GetTargetTxId(), FirstTxId);
        Acknowledgements.Unblock(1);
    }

    void RejectSecond(ui64 txId) {
        auto proposal = std::move(SecondProposal.front());
        SecondProposal.pop_front();
        SecondProposal.Stop();

        auto rejected = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            NKikimrScheme::StatusSchemeError, txId, TTestTxConfig::SchemeShard);
        rejected->Record.SetReason("Injected proposal rejection");
        Test.Runtime.Send(new IEventHandle(proposal->Sender, proposal->Recipient, rejected.Release()), 0, true);
    }

    void AcceptSecond(ui64 txId) {
        SecondProposal.Stop().Unblock();
        Test.Runtime.WaitFor("second backup cancellation", [&] {
            return GetCancellationTargets(Acknowledgements).contains(txId);
        });
    }
};

struct TDelayedDataUpload {
    TBlockEvents<NWrappers::TEvExternalStorage::TEvPutObjectRequest> Requests;

    explicit TDelayedDataUpload(TExportEnv& test)
        : Requests(test.Runtime, [](const auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        })
    {}
};

void SchemeMatchesDataSnapshot(bool isFs, bool reboot, bool isColumn, bool dropSource) {
    TExportEnv test(isFs);
    if (isColumn) {
        test.CreateColumnTable();
    } else {
        test.CreateTable();
    }

    TDelayedCopy copy(test);

    const ui64 exportId = test.Export();
    copy.WaitForCopy();
    UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);

    if (dropSource) {
        if (isColumn) {
            TestDropColumnTable(test.Runtime, ++test.TxId, "/MyRoot", "Table");
        } else {
            TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", "Table");
        }
    } else if (isColumn) {
        TestAlterColumnTable(test.Runtime, ++test.TxId, "/MyRoot", R"(
            Name: "Table"
            AlterSchema { AddColumns { Name: "extra" Type: "Utf8" } }
        )");
    } else {
        WriteRow(test.Runtime, ++test.TxId, "/MyRoot/Table", 0, 1, "after");
        TestAlterTable(test.Runtime, ++test.TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "extra" Type: "Utf8" }
        )");
    }

    test.Env.TestWaitNotification(test.Runtime, test.TxId);

    copy.Resume(reboot);
    test.Env.TestWaitNotification(test.Runtime, exportId);
    test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

    const auto scheme = test.ReadScheme();

    UNIT_ASSERT_VALUES_EQUAL(scheme.columns_size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(scheme.columns(0).name(), "key");
    UNIT_ASSERT_VALUES_EQUAL(scheme.columns(1).name(), "value");

    if (!isColumn) {
        UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("data_00.csv"), "1,\"before\"\n");
    } else {
        UNIT_ASSERT(!test.ReadFile("data_00.csv").empty());
    }

    test.Forget(exportId);
    test.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
}

enum class ESchemeShardVersionSwitch {
    None,
    Upgrade,
    Downgrade,
};

void SchemeAttributeAfterVersionSwitch(bool isFs, ESchemeShardVersionSwitch versionSwitch, const char* expectedAttribute) {
    NSchemeShard::TSchemeShard* schemeShard = nullptr;
    TExportEnv test(isFs, [&](const TActorId& tablet, TTabletStorageInfo* info) {
        schemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
        return schemeShard;
    });
    test.CreateTable();

    auto setAttribute = [&](const TString& value) {
        TestUserAttrs(test.Runtime, ++test.TxId, "/MyRoot", "Table", AlterUserAttrs({{"version_switch", value}}));
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
    };
    setAttribute("before");

    TDelayedCopy copy(test);

    const ui64 exportId = test.Export();
    copy.WaitForCopy(versionSwitch != ESchemeShardVersionSwitch::Upgrade);
    const ui64 snapshotCount = versionSwitch == ESchemeShardVersionSwitch::Upgrade ? 0 : 1;
    UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), snapshotCount);

    setAttribute("after");
    TestDescribeResult(DescribePath(test.Runtime, "/MyRoot/Table"),
        {NLs::UserAttrsEqual({{"version_switch", "after"}})});

    // Switch while the consistent copy is complete and no backup has been proposed.
    copy.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), snapshotCount);
    if (versionSwitch == ESchemeShardVersionSwitch::Downgrade) {
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();
        // Model an old SchemeShard ignoring the additive snapshot table on startup.
        // Leave the persisted row intact; only discard the snapshot loaded in memory.
        schemeShard->Tables.at(TPathId(TTestTxConfig::SchemeShard, pathId))->BackupSettings.Clear();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);
    }
    copy.Resume(false);
    test.Env.TestWaitNotification(test.Runtime, exportId);

    test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

    const auto scheme = test.ReadScheme();
    UNIT_ASSERT_C(scheme.attributes().contains("version_switch"), scheme.DebugString());
    UNIT_ASSERT_VALUES_EQUAL(scheme.attributes().at("version_switch"), expectedAttribute);
    UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("data_00.csv"), "1,\"before\"\n");
    UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), snapshotCount);

    test.Forget(exportId);
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardExportTableSchemeSnapshotTests) {

    Y_UNIT_TEST(ExportFixturePreservesBackupQueueDefaults) {
        TExportEnv test(true);
        const auto config = NResourceBroker::MakeDefaultConfig();
        const auto queue = FindIf(config.GetQueues(), [](const auto& queue) {
            return queue.GetName() == "queue_backup";
        });
        UNIT_ASSERT(queue != config.GetQueues().end());

        for (ui32 nodeIdx = 0; nodeIdx < test.Runtime.GetNodeCount(); ++nodeIdx) {
            const auto sender = test.Runtime.AllocateEdgeActor(nodeIdx);
            test.Runtime.Send(new IEventHandle( test.Runtime.GetLocalServiceId(NResourceBroker::MakeResourceBrokerID(), nodeIdx), sender,
                new NResourceBroker::TEvResourceBroker::TEvConfigRequest("queue_backup")), nodeIdx);
            const auto reply = test.Runtime.GrabEdgeEvent<NResourceBroker::TEvResourceBroker::TEvConfigResponse>(sender);
            UNIT_ASSERT(reply->Get()->QueueConfig);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->QueueConfig->GetLimit().GetCpu(), queue->GetLimit().GetCpu());
        }
    }

    Y_UNIT_TEST(BackupCopiesOutsideExportDoNotCaptureScheme) {
        TExportEnv test(false);
        test.CreateTable();
        TestConsistentCopyTables(test.Runtime, ++test.TxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/Backup"
                IsBackup: true
                OmitIndexes: true
            }
        )");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
    }

    Y_UNIT_TEST(RegularCopyCanCaptureTableSchemeSnapshot) {
        TExportEnv test(false);
        test.CreateTable();
        TestConsistentCopyTables(test.Runtime, ++test.TxId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/Copy"
                CaptureTableSchemeSnapshot: true
                OmitIndexes: true
            }
        )");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);

        test.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);

        TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", "Copy");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
    }

    Y_UNIT_TEST(OrphanTableSchemeSnapshotIsRemovedAfterReboot) {
        TExportEnv test(false);
        LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '999999))
                ))
                (let description '('TableDescription (String '"")))
                (let topics '('ChangefeedUnderlyingTopics (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(description topics))))
            )
        )", TTestTxConfig::SchemeShard));
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);
        test.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
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

    Y_UNIT_TEST_TWIN(SchemeAttributesUseTableSchemeSnapshot, IsFs) {
        SchemeAttributeAfterVersionSwitch(IsFs, ESchemeShardVersionSwitch::None, "before");
    }

    Y_UNIT_TEST_TWIN(UpgradeBeforeBackupUsesLiveScheme, IsFs) {
        SchemeAttributeAfterVersionSwitch(IsFs, ESchemeShardVersionSwitch::Upgrade, "after");
    }

    Y_UNIT_TEST_TWIN(DowngradeBeforeBackupIgnoresTableSchemeSnapshot, IsFs) {
        SchemeAttributeAfterVersionSwitch(IsFs, ESchemeShardVersionSwitch::Downgrade, "after");
    }

    Y_UNIT_TEST_TWIN(InvalidTableSchemeSnapshotCancelsWholeExport, IsFs) {
        TExportEnv test(IsFs);
        test.CreateTable();
        test.CreateKeyTable("Other");

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("", {"Other"});
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 2);
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();

        // A valid protobuf without a table must fail preparation after recovery
        UpdateTableSchemeSnapshot(test, pathId, "TableDescription", "");
        copy.Resume(true);
        test.Env.TestWaitNotification(test.Runtime, exportId);

        const auto entry = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "snapshot");
        UNIT_ASSERT(!test.HasFile("scheme.pb"));
        UNIT_ASSERT(!test.HasExportCompletionMarker());

        test.Reboot();

        test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptTableSchemeSnapshotCancelsOnlyItsExport, IsFs, CorruptTopics) {
        TExportEnv test(IsFs);
        // Operation timestamps are persisted with second precision.
        test.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        test.CreateTable();
        if (CorruptTopics) {
            test.CreateChangefeed();
        }

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export();
        copy.WaitForCopy();
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();
        CorruptTableSchemeSnapshot(test, pathId, CorruptTopics);
        copy.Resume(true);
        test.Env.TestWaitNotification(test.Runtime, exportId);

        const TString error = CorruptTopics ? "changefeed topics" : "table scheme snapshot";
        auto checkCancelled = [&] {
            const auto entry = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT(entry.HasEndTime());
            UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), error);
            UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);
        };
        checkCancelled();
        UNIT_ASSERT(!test.HasFile("scheme.pb"));
        UNIT_ASSERT(!test.HasExportCompletionMarker());
        test.Reboot();
        checkCancelled();

        const ui64 healthyExportId = test.Export();
        test.Env.TestWaitNotification(test.Runtime, healthyExportId);
        test.GetExportEntry(healthyExportId, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("data_00.csv"), "1,\"before\"\n");
        TestForgetExport(test.Runtime, ++test.TxId, "/MyRoot", healthyExportId);
        test.Env.TestWaitNotification(test.Runtime, healthyExportId);
        test.GetExportEntry(healthyExportId, Ydb::StatusIds::NOT_FOUND);
        checkCancelled();

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsActiveBackupsAcrossRestarts, IsFs, CorruptIndex) {
        TExportEnv test(IsFs);
        test.CreateIndexedTable();

        TDelayedDataUpload upload(test);
        test.ConfigureBackupConcurrency(2);

        TBlockEvents<TEvSchemeShard::TEvCancelTx> cancellations(test.Runtime);

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy();
        const TString root = test.ExportPath(exportId);
        const TString brokenPath = CorruptIndex ? root + "/index/indexImplTable" : root;
        const auto pathId = DescribePrivatePath(test.Runtime, brokenPath).GetPathId();

        copy.Resume(false);
        test.Runtime.WaitFor("both copied tables uploading", [&] {
            return GetObjectKeys(upload.Requests).size() == 2;
        });
        CorruptTableSchemeSnapshot(test, pathId);

        for (ui32 restart = 0; restart < 2; ++restart) {
            cancellations.clear();
            test.Reboot();
            test.Runtime.WaitFor("both backup cancellations after recovery", [&] {
                return GetCancellationTargets(cancellations).size() == 2;
            });

            const auto result = test.GetExportEntry(exportId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
            UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 2);
            UNIT_ASSERT(!test.HasExportCompletionMarker());
        }
        cancellations.Stop().Unblock();
        upload.Requests.clear();
        upload.Requests.Stop();
        test.Env.TestWaitNotification(test.Runtime, exportId);

        const auto result = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "table scheme snapshot");
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 2);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(BackupAcceptanceRepliesLostBeforeRestart, IsFs, BackupFinished) {
        TExportEnv test(IsFs);
        test.CreateIndexedTable();

        const auto expectedRows = test.ReadRows("/MyRoot/Table");
        const auto expectedIndex = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        TBackupReplies backups(test);
        TDelayedDataUpload upload(test);
        test.ConfigureBackupConcurrency(2);

        const ui64 exportId = test.Export("include_index_data: true");
        test.Runtime.WaitFor("both accepted backups uploading without acceptance replies", [&] {
            return GetObjectKeys(upload.Requests).size() == 2 && backups.Replies.size() == 2;
        });
        UNIT_ASSERT_VALUES_EQUAL(backups.TxIds.size(), 2);
        backups.CheckAccepted();
        UNIT_ASSERT_VALUES_EQUAL(ReadExportBackupTxIds(test, exportId), (TVector<ui64>{0, 0}));
        if (BackupFinished) {
            upload.Requests.Stop().Unblock();

            for (const auto txId : backups.TxIds) {
                test.Env.TestWaitNotification(test.Runtime, txId);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReadExportBackupTxIds(test, exportId), (TVector<ui64>{0, 0}));
            UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("data_00.csv"), "1,\"before\"\n");
            UNIT_ASSERT(!test.HasExportCompletionMarker());
        }
        backups.DiscardReplies();

        for (ui32 restart = 0; restart < 2; ++restart) {
            test.Reboot();
            if (BackupFinished) {
                test.Env.TestWaitNotification(test.Runtime, exportId);
                test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
                continue;
            }

            test.Runtime.WaitFor("both backup transaction ids recovered", [&] {
                const auto ids = ReadExportBackupTxIds(test, exportId);
                return ids.size() == 2 && ids[0] && ids[1];
            });
            backups.CheckRecoveredTxIds(exportId);
            UNIT_ASSERT(!test.HasExportCompletionMarker());
        }
        if (!BackupFinished) {
            upload.Requests.Stop().Unblock();
        }

        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
        if (!IsFs) {
            UNIT_ASSERT(test.S3.GetData().contains("/backup/export_complete.json"));
            UNIT_ASSERT(test.HasExportCompletionMarker());
        }

        test.Import(true);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored"), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsAcceptedBackupsAcrossRestarts, IsFs, LoseAcceptanceReplies) {
        TExportEnv test(IsFs);
        test.CreateIndexedTable();

        TBackupReplies backups(test, LoseAcceptanceReplies);
        TDelayedDataUpload upload(test);
        test.ConfigureBackupConcurrency(2);
        // Hold the response after the real cancel handler has persisted cancellation.
        TBlockEvents<TEvSchemeShard::TEvCancelTxResult> acknowledgements(test.Runtime);

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy();
        const TString root = test.ExportPath(exportId);
        const auto pathId = DescribePrivatePath(test.Runtime, root + "/index/indexImplTable").GetPathId();
        copy.Resume(false);
        test.Runtime.WaitFor("both backups active before damaging the snapshot", [&] {
            return GetObjectKeys(upload.Requests).size() == 2 && (!LoseAcceptanceReplies || backups.Replies.size() == 2);
        });
        UNIT_ASSERT_VALUES_EQUAL(backups.TxIds.size(), 2);
        if (LoseAcceptanceReplies) {
            UNIT_ASSERT_VALUES_EQUAL(ReadExportBackupTxIds(test, exportId), (TVector<ui64>{0, 0}));
            backups.CheckAccepted();
        }
        backups.DiscardReplies();
        CorruptTableSchemeSnapshot(test, pathId);

        for (ui32 restart = 0; restart < 2; ++restart) {
            acknowledgements.clear();
            test.Reboot();
            test.Runtime.WaitFor("both cancellation acknowledgements after recovery", [&] {
                return GetCancellationTargets(acknowledgements).size() == 2;
            });

            for (const auto& ack : acknowledgements) {
                UNIT_ASSERT(backups.TxIds.contains(ack->Get()->Record.GetTargetTxId()));
                if (!restart) {
                    UNIT_ASSERT_VALUES_EQUAL(ack->Get()->Record.GetStatus(), NKikimrScheme::StatusAccepted);
                }
            }
            backups.CheckRecoveredTxIds(exportId);

            const auto result = test.GetExportEntry(exportId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
            UNIT_ASSERT(!test.HasExportCompletionMarker());
            if (!restart) {
                RebootTableShards(test, root);
                RebootTableShards(test, root + "/index/indexImplTable");
            }
        }
        acknowledgements.Stop().Unblock();
        upload.Requests.clear();
        upload.Requests.Stop();
        test.Env.TestWaitNotification(test.Runtime, exportId);

        for (const auto txId : backups.TxIds) {
            test.Env.TestWaitNotification(test.Runtime, txId);
        }
        auto checkCancelled = [&] {
            const auto result = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "table scheme snapshot");
            UNIT_ASSERT(!test.HasExportCompletionMarker());
            UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 2);
        };
        checkCancelled();
        test.Reboot();
        checkCancelled();

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(CorruptColumnTableSnapshotCancelsExport, ActiveBackup) {
        TExportEnv test(false);
        test.CreateColumnTable();

        TDelayedDataUpload upload(test);

        TBlockEvents<TEvSchemeShard::TEvCancelTxResult> acknowledgements(test.Runtime);

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export();
        copy.WaitForCopy();
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();
        if (ActiveBackup) {
            copy.Resume(false);
            test.Runtime.WaitFor("column table data upload", [&] { return !upload.Requests.empty(); });
        }
        CorruptTableSchemeSnapshot(test, pathId);
        if (ActiveBackup) {
            for (ui32 restart = 0; restart < 2; ++restart) {
                acknowledgements.clear();
                test.Reboot();
                test.Runtime.WaitFor("column table backup cancellation", [&] { return !acknowledgements.empty(); });

                const auto result = test.GetExportEntry(exportId);
                UNIT_ASSERT_VALUES_EQUAL(result.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
            }
        } else {
            copy.Resume(true);
        }
        acknowledgements.Stop().Unblock();
        upload.Requests.clear();
        upload.Requests.Stop();
        test.Env.TestWaitNotification(test.Runtime, exportId);

        const auto result = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "table scheme snapshot");
        UNIT_ASSERT(!test.HasExportCompletionMarker());
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);
        test.Reboot();
        test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);

        test.Forget(exportId);
        const ui64 healthyExportId = test.Export();
        test.Env.TestWaitNotification(test.Runtime, healthyExportId);
        test.GetExportEntry(healthyExportId, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(test.HasExportCompletionMarker());
        UNIT_ASSERT(!test.ReadFile("data_00.csv").empty());

        test.Forget(healthyExportId);
    }

    Y_UNIT_TEST(CorruptSnapshotDoesNotCancelExportAwaitingOperationMarker) {
        TExportEnv test(false);
        test.CreateIndexedTable();

        const auto expectedIndex = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        TBlockEvents<NSchemeShard::TEvPrivate::TEvExportCompletionUploadResult> markerReplies(test.Runtime,
            [](const auto& ev) { return ev->Get()->ItemIdx == Max<ui32>(); });

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy();
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();
        copy.Resume(false);
        test.Runtime.WaitFor("operation completion marker uploaded", [&] { return !markerReplies.empty(); });
        UNIT_ASSERT(test.HasExportCompletionMarker());
        UNIT_ASSERT_VALUES_EQUAL(ReadExportItemState(test, exportId, 0), NSchemeShard::TExportInfo::EState::Done);
        UNIT_ASSERT_VALUES_EQUAL(ReadExportItemState(test, exportId, 1), NSchemeShard::TExportInfo::EState::Done);
        CorruptTableSchemeSnapshot(test, pathId);

        markerReplies.clear();
        markerReplies.Stop();

        for (ui32 restart = 0; restart < 2; ++restart) {
            test.Reboot();
            test.Env.TestWaitNotification(test.Runtime, exportId);
            test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
        }

        test.Import(true);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(MissingChangefeedTopicsCancelsWholeExport, IsFs) {
        TExportEnv test(IsFs);
        test.CreateTable();
        test.CreateChangefeed();

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export();
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 1);
        const auto pathId = DescribePrivatePath(test.Runtime, test.ExportPath(exportId)).GetPathId();

        // Keep the captured CDC stream, but remove its underlying topic from the snapshot.
        UpdateTableSchemeSnapshot(test, pathId, "ChangefeedUnderlyingTopics", "");
        copy.Resume(true);
        test.Env.TestWaitNotification(test.Runtime, exportId);

        const auto entry = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "changefeed topics");
        UNIT_ASSERT(!test.HasFile("scheme.pb"));
        UNIT_ASSERT(!test.HasExportCompletionMarker());

        test.Reboot();

        test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(DroppedSourceBeforeCaptureCancelsExport, IsFs) {
        TExportEnv test(IsFs);
        test.CreateTable();

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export();
        test.Runtime.WaitFor("consistent copy proposal", [&] { return !copy.Proposal.empty(); });

        TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", "Table");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        copy.Proposal.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, exportId);

        test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
        UNIT_ASSERT(!test.HasFile("scheme.pb"));

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(IndexesAndChangefeedsSurviveSourceDrop, IsFs) {
        TExportEnv test(IsFs);
        test.CreateIndexedTable();
        test.CreateChangefeed();

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 2);
        TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", "Table");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);

        copy.Resume(true);
        test.Env.TestWaitNotification(test.Runtime, exportId);

        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        const auto scheme = test.ReadScheme();

        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");

        UNIT_ASSERT(!test.ReadFile("feed/changefeed_description.pb").empty());
        UNIT_ASSERT(!test.ReadFile("feed/topic_description.pb").empty());

        const auto indexScheme = test.ReadScheme("index/indexImplTable/scheme.pb");
        UNIT_ASSERT_VALUES_EQUAL(indexScheme.primary_key_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(indexScheme.primary_key(0), "value");
        UNIT_ASSERT_VALUES_EQUAL(indexScheme.primary_key(1), "key");

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(SchemeFailureWaitsForProposedSibling, RejectProposal) {
        TExportEnv test(false);
        test.CreateTable();

        for (const TString name : {"Other", "Another"}) {
            test.CreateKeyTable(name);
        }

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("", {"Other", "Another"});
        copy.WaitForCopy();

        const auto pathId = DescribePrivatePath(test.Runtime, Sprintf("/MyRoot/export-%lu/2", exportId)).GetPathId();
        UpdateTableSchemeSnapshot(test, pathId, "TableDescription", "");

        TProposedBackups backups(test, exportId);

        copy.Resume(true);
        backups.StartFirst();
        const ui64 secondBackupTxId = backups.StartSecond();
        backups.FailThird();

        const auto entry = test.GetExportEntry(exportId);
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(!entry.HasEndTime());

        if (RejectProposal) {
            backups.RejectSecond(secondBackupTxId);
        } else {
            backups.AcceptSecond(secondBackupTxId);
        }

        backups.Acknowledgements.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(!test.HasExportCompletionMarker());

        backups.SchemaChanged.Stop().Unblock();
        backups.Subscriptions.Stop().Unblock();

        test.Forget(exportId);
    }
}
