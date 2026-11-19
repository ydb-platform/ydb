#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_flow_proposals.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/value/value.h>

#include <google/protobuf/text_format.h>

#include <functional>
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
    bool Encrypted = false;
    bool BuildIndexesOnImport = false;
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

    void CreateVectorTable(const TString& name = "Table") {
        TestCreateTable(Runtime, ++TxId, "/MyRoot", Sprintf(R"(
            Name: "%s"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )", name.c_str()));
        Env.TestWaitNotification(Runtime, TxId);

        WriteVectorTableRows(Runtime, TTestTxConfig::SchemeShard, ++TxId, "/MyRoot/" + name, 0, 0, 200);
    }

    ui64 CreateIndex(const TString& name, const TVector<TString>& keys = {"value"}, const TString& table = "/MyRoot/Table") {
        const ui64 buildId = ++TxId;
        TestBuildIndex(Runtime, buildId, TTestTxConfig::SchemeShard, "/MyRoot", table, name, keys);
        Env.TestWaitNotification(Runtime, buildId);
        return buildId;
    }

    ui64 CreateVectorIndex(const TString& table = "/MyRoot/Table") {
        const ui64 buildId = ++TxId;
        TestBuildVectorIndex(Runtime, buildId, TTestTxConfig::SchemeShard, "/MyRoot", table, "index", {"embedding"});
        Env.TestWaitNotification(Runtime, buildId);
        return buildId;
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

    auto GetImportEntry(ui64 importId, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        return TestGetImport(Runtime, importId, "/MyRoot", status).GetResponse().GetEntry();
    }

    void CheckBuildState(ui64 buildId, Ydb::Table::IndexBuildState::State state) {
        const auto result = TestGetBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), state, result.DebugString());
    }

    TString EncryptionSettings() const {
        return Encrypted ? R"(
            encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key { key: "0123456789012345" }
            }
        )" : "";
    }

    ui64 Export(const TString& settings = "", const TVector<TString>& extraTables = {}) {
        if (Encrypted) {
            Runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);
        }
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
                    %s %s %s
                }
            )", Directory.Path().c_str(), settings.c_str(), extraItems.c_str(), EncryptionSettings().c_str())
            : Sprintf(R"(
                ExportToS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { source_path: "/MyRoot/Table" destination_prefix: "backup" }
                    %s %s %s %s
                }
            )", Port, settings.c_str(), extraItems.c_str(), EncryptionSettings().c_str(),
                Encrypted ? "destination_prefix: \"Backup\"" : "");
        TestExport(Runtime, ++TxId, "/MyRoot", request);
        return TxId;
    }

    TString ImportSettings(const TVector<TString>& extraTables) {
        const char* indexPopulationMode = BuildIndexesOnImport ? "INDEX_POPULATION_MODE_BUILD" : "INDEX_POPULATION_MODE_IMPORT";
        const char* sourceField = Encrypted ? (IsFs ? "source_path_db" : "source_path")
            : (IsFs ? "source_path" : "source_prefix");
        const char* source = Encrypted ? "Table" : "backup";

        TStringBuilder extraItems;

        for (const auto& name : extraTables) {
            extraItems << "items { " << sourceField << ": \"" << name
                << "\" destination_path: \"/MyRoot/Restored" << name << "\" }\n";
        }
        const TString request = IsFs
            ? Sprintf(R"(
                ImportFromFsSettings {
                    base_path: "%s"
                    items { %s: "%s" destination_path: "/MyRoot/Restored" }
                    index_population_mode: %s
                    %s %s
                }
            )", Directory.Path().c_str(), sourceField, source, indexPopulationMode, extraItems.c_str(), EncryptionSettings().c_str())
            : Sprintf(R"(
                ImportFromS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { %s: "%s" destination_path: "/MyRoot/Restored" }
                    index_population_mode: %s
                    %s %s %s
                }
            )", Port, sourceField, source,
                indexPopulationMode, extraItems.c_str(), EncryptionSettings().c_str(), Encrypted ? "source_prefix: \"Backup\"" : "");
        return request;
    }

    void Import(bool rebootBeforeRestore = false, const TVector<TString>& extraTables = {},
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const std::function<void(ui64)>& interrupt = {})
    {
        const ui64 importId = TxId + 1;
        bool schemeFailed = false;
        auto schemeResults = Runtime.AddObserver<NSchemeShard::TEvPrivate::TEvImportSchemeReady>([&](const auto& ev) {
            schemeFailed |= ev->Get()->ImportId == importId && !ev->Get()->Success;
        });

        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> allocations(Runtime, [=](const auto& ev) {
            return rebootBeforeRestore && ev->Cookie == importId;
        });
        TestImport(Runtime, ++TxId, "/MyRoot", ImportSettings(extraTables));
        if (rebootBeforeRestore) {
            // Scheme getter has committed the children and their IVs, but no
            // creation/restore transaction has received its allocated ID yet.
            Runtime.WaitFor("import allocation or scheme failure", [&] { return !allocations.empty() || schemeFailed; });
            if (!schemeFailed) {
                allocations.clear();
                if (interrupt) {
                    interrupt(importId);
                }
                RebootTablet(Runtime, TTestTxConfig::SchemeShard, Runtime.AllocateEdgeActor());
            }
            allocations.Stop().Unblock();
        } else if (interrupt) {
            interrupt(importId);
        }
        Env.TestWaitNotification(Runtime, TxId);
        TestGetImport(Runtime, TxId, "/MyRoot", expectedStatus);
    }

    TString ReadRawFile(const TString& name, const TString& table = "backup") {
        if (IsFs) {
            return TFileInput(Directory.Path() / table / name).ReadAll();
        }

        const auto& data = S3.GetData();
        const auto* content = data.FindPtr((Encrypted ? "/Backup/" : "/") + table + "/" + name);
        UNIT_ASSERT_C(content, "Missing backup file: " << name);
        return *content;
    }

    TString ReadFile(const TString& name, const TString& table = "backup") {
        const auto content = ReadRawFile(name + (Encrypted ? ".enc" : ""), table);
        if (!Encrypted) {
            return content;
        }
        auto [data, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
            NBackup::TEncryptionKey("0123456789012345"), TBuffer(content.data(), content.size()));
        return TString(data.Data(), data.Size());
    }

    bool HasFile(const TString& name, const TString& table = "backup") {
        const TString file = name + (Encrypted ? ".enc" : "");
        return IsFs ? (Directory.Path() / table / file).Exists()
            : S3.GetData().contains((Encrypted ? "/Backup/" : "/") + table + "/" + file);
    }

    bool HasExportCompletionMarker() {
        return !IsFs && AnyOf(S3.GetData(), [](const auto& object) {
            return object.first.EndsWith("/export_complete.json");
        });
    }

    void CheckEncryptionIVs() {
        THashMap<TString, TString> files;
        if (IsFs) {
            TVector<TFsPath> pending = {Directory.Path()};
            while (!pending.empty()) {
                const auto path = pending.back();
                pending.pop_back();
                if (path.IsDirectory()) {
                    path.List(pending);
                } else if (path.GetPath().EndsWith(".enc")) {
                    files.emplace(path.GetPath(), TFileInput(path).ReadAll());
                }
            }
        } else {
            for (const auto& [name, data] : S3.GetData()) {
                if (name.EndsWith(".enc")) {
                    files.emplace(name, data);
                }
            }
        }
        UNIT_ASSERT(!files.empty());
        THashSet<TString> ivs;

        for (const auto& [name, data] : files) {
            const auto [plain, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                NBackup::TEncryptionKey("0123456789012345"), TBuffer(data.data(), data.size()));
            UNIT_ASSERT_C(ivs.insert(iv.GetBinaryString()).second, "Reused IV: " << name);
        }
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

TString CorruptImportMetadata(TExportEnv& test, ui64 importId, bool loseWaitTxId = false) {
    const TString metadata = R"({"indexes":[{"export_prefix":"001","impl_table_prefix":"index/indexImplTable","iv":""}]})";
    LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let key '('('ImportId (Uint64 '%lu)) '('Index (Uint32 '0))))
            (let metadata '('Metadata (String '"%s")))
            (return (AsList (UpdateRow 'ImportItems key '(metadata))))
        )
    )", importId, EscapeC(metadata).c_str()));
    if (loseWaitTxId) {
        LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('ImportId (Uint64 '%lu)) '('Index (Uint32 '0))))
                (let waitTxId '('WaitTxId (Uint64 '0)))
                (return (AsList (UpdateRow 'ImportItems key '(waitTxId))))
            )
        )", importId));
    }

    return metadata;
}

NKikimrMiniKQL::TResult ReadImportItems(TExportEnv& test, ui64 importId) {
    return LocalMiniKQL(test.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let range '(
                '('ImportId (Uint64 '%lu) (Uint64 '%lu))
                '('Index (Null) (Void))
            ))
            (return (AsList (SetResult 'Result
                (SelectRange 'ImportItems range '('Metadata 'ParentIndex) '()))))
        )
    )", importId, importId));
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

    void ResumeWithRebootBeforeBackup(ui64 exportId) {
        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> allocations(Test.Runtime, [exportId](const auto& ev) {
            return ev->Cookie == exportId;
        });
        Resume(false);
        // The refreshed index items must survive a restart before any backup starts.
        Test.Runtime.WaitFor("backup allocation", [&] { return !allocations.empty(); });
        allocations.clear();
        Test.Reboot();
        allocations.Stop().Unblock();
    }
};

enum class ERestart {
    None,
    SchemeShard,
    DataShard,
    Both,
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

struct TImportWork {
    TExportEnv& Test;
    const bool BuildIndex;
    ui64 BuildId = 0;
    TBlockEvents<NWrappers::TEvExternalStorage::TEvGetObjectRequest> DataReads;
    TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> Scans;
    TBlockEvents<TEvSchemeShard::TEvCancelTx> RestoreCancellations;
    TBlockEvents<TEvIndexBuilder::TEvCancelRequest> BuildCancellations;

    TImportWork(TExportEnv& test, bool buildIndex)
        : Test(test)
        , BuildIndex(buildIndex)
        , DataReads(test.Runtime, [this](const auto& ev) {
            const TString key(ev->Get()->Request.GetKey());
            return !BuildIndex && (key.EndsWith(".csv") || key.EndsWith(".csv.enc"));
        })
        , Scans(test.Runtime, [this](const auto&) { return BuildIndex; })
        , RestoreCancellations(test.Runtime)
        , BuildCancellations(test.Runtime)
    {}

    void WaitForStart() {
        Test.Runtime.WaitFor("active import work", [&] {
            return BuildIndex ? !Scans.empty() : GetObjectKeys(DataReads).size() == 2;
        });
        if (BuildIndex) {
            BuildId = Scans.front()->Get()->Record.GetId();
        }
    }

    void ClearCancellationRequests() {
        RestoreCancellations.clear();
        BuildCancellations.clear();
    }

    void WaitForCancellation() {
        Test.Runtime.WaitFor("active import cancellation after recovery", [&] {
            return BuildIndex ? !BuildCancellations.empty() : GetCancellationTargets(RestoreCancellations).size() == 2;
        });
    }

    void Release() {
        RestoreCancellations.Stop().Unblock();
        BuildCancellations.Stop().Unblock();
        DataReads.clear();
        DataReads.Stop();
        Scans.clear();
        Scans.Stop();
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
    TExportEnv& Test;
    TBlockEvents<NWrappers::TEvExternalStorage::TEvPutObjectRequest> Requests;

    explicit TDelayedDataUpload(TExportEnv& test)
        : Test(test)
        , Requests(test.Runtime, [](const auto& ev) {
            const TString key(ev->Get()->Request.GetKey());
            return key.EndsWith(".csv") || key.EndsWith(".csv.enc");
        })
    {}

    void StartAndReboot(TDelayedCopy& copy, ui64 exportId, const TVector<TString>& implTables) {
        // Every copied table must reach the upload barrier before any is released.
        Test.ConfigureBackupConcurrency(implTables.size() + 1);
        copy.ResumeWithRebootBeforeBackup(exportId);
        Reboot(exportId, implTables);
    }

    void Reboot(ui64 exportId, const TVector<TString>& implTables) {
        Test.Runtime.WaitFor("all copied tables uploading while index build is active", [&] {
            return GetObjectKeys(Requests).size() >= implTables.size() + 1;
        });
        THashSet<TActorId> oldUploaders;

        for (const auto& request : Requests) {
            oldUploaders.insert(request->Sender);
        }
        Requests.clear();
        const TString root = Test.ExportPath(exportId);
        RebootTableShards(Test, root);

        for (const auto& implTable : implTables) {
            RebootTableShards(Test, root + "/" + implTable);
        }

        Test.Reboot();
        Test.Runtime.WaitFor("data upload from a restored uploader", [&] {
            for (const auto& request : Requests) {
                if (!oldUploaders.contains(request->Sender)) {
                    return true;
                }
            }

            return false;
        });
    }
};

auto EmulateLegacyUploader(TExportEnv& test, bool enabled, ui32& oldUploaderTasks) {
    return test.Runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([enabled, &oldUploaderTasks](auto& ev) {
        auto& record = ev->Get()->Record;
        if (!enabled || record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
            return;
        }
        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
        if (tx.HasBackup() && tx.GetBackup().GetTable().GetTable().TableIndexesSize()) {
            // Old DataShards ignore this additive field, but honor the child's IV.
            tx.MutableBackup()->MutableEncryptionSettings()->ClearIndexIVs();
            record.SetTxBody(tx.SerializeAsString());
            ++oldUploaderTasks;
        }
    });
}

THashMap<TString, TString> MaterializedIndexPrefixes(TExportEnv& test,
        const THashSet<TString>& expectedImplTables, const TString& table = "backup", bool expectMetadataIV = true)
{
    const auto metadata = NBackup::TMetadata::Deserialize(test.ReadFile("metadata.json", table));
    UNIT_ASSERT(metadata.GetIndexes());
    UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), expectedImplTables.size());
    THashMap<TString, TString> prefixes;

    for (const auto& index : *metadata.GetIndexes()) {
        UNIT_ASSERT_C(expectedImplTables.contains(index.ImplTablePrefix), index.ImplTablePrefix);
        UNIT_ASSERT(prefixes.emplace(index.ImplTablePrefix, index.ExportPrefix).second);
        UNIT_ASSERT(test.HasFile(index.ExportPrefix + "/scheme.pb", table));
        UNIT_ASSERT(test.HasFile(index.ExportPrefix + "/data_00.csv", table));
        if (test.Encrypted) {
            UNIT_ASSERT_VALUES_EQUAL(bool(index.IV), expectMetadataIV);
            if (index.IV) {
                const auto content = test.ReadRawFile(index.ExportPrefix + "/scheme.pb.enc", table);
                const auto expectedIV = NBackup::TEncryptionIV::Combine(*index.IV,
                    NBackup::EBackupFileType::TableSchema, 0, 0);
                UNIT_ASSERT_NO_EXCEPTION(NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                    NBackup::TEncryptionKey("0123456789012345"), expectedIV, TBuffer(content.data(), content.size())));
            }
        }
    }

    return prefixes;
}

TVector<ui64> CreateIndexBuildHistory(TExportEnv& test) {
    TestCreateTable(test.Runtime, ++test.TxId, "/MyRoot", TExportEnv::RowTableDescription("History"));
    test.Env.TestWaitNotification(test.Runtime, test.TxId);
    WriteRow(test.Runtime, ++test.TxId, "/MyRoot/History", 0, 1, "history");
    TVector<ui64> history;

    for (ui32 i = 0; i < 3; ++i) {
        const ui64 buildId = test.CreateIndex("history-index", {"value"}, "/MyRoot/History");
        test.CheckBuildState(buildId, Ydb::Table::IndexBuildState::STATE_DONE);
        history.push_back(buildId);
        TestDropTableIndex(test.Runtime, TTestTxConfig::SchemeShard, ++test.TxId, "/MyRoot", R"(
            TableName: "History"
            IndexName: "history-index"
        )");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
    }

    return history;
}

void ChangeIndexBackupIV(TExportEnv& test, const TString& prefix) {
    const auto changeBackupIV = [](const TString& encrypted) {
        auto [plain, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
            NBackup::TEncryptionKey("0123456789012345"), TBuffer(encrypted.data(), encrypted.size()));
        // Keep all child files mutually consistent and authenticated, but
        // make their IV namespace differ from the parent backup's namespace.
        iv.IV[NBackup::TEncryptionIV::FILE_TYPE_OFFSET] ^= 0x80;
        const auto foreign = NBackup::TEncryptedFileSerializer::EncryptFullFile("AES-128-GCM",
            NBackup::TEncryptionKey("0123456789012345"), iv, TStringBuf(plain.Data(), plain.Size()));
        return TString(foreign.Data(), foreign.Size());
    };
    if (test.IsFs) {
        TVector<TFsPath> files;
        (test.Directory.Path() / "backup" / prefix).List(files);

        for (const auto& file : files) {
            if (file.GetPath().EndsWith(".enc")) {
                const auto content = changeBackupIV(TFileInput(file).ReadAll());
                TFileOutput(file).Write(content);
            }
        }
    } else {
        for (auto& [name, content] : test.S3.GetData()) {
            if (name.StartsWith("/Backup/backup/" + prefix + "/") && name.EndsWith(".enc")) {
                content = changeBackupIV(content);
            }
        }
    }
}

void CreateShardedTableWithAsyncIndex(TExportEnv& test, const TString& name) {
    TestCreateIndexedTable(test.Runtime, ++test.TxId, "/MyRoot", Sprintf(R"(
        TableDescription {
            Name: "%s"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        }
        IndexDescription {
            Name: "a_async"
            KeyColumnNames: ["value"]
            Type: EIndexTypeGlobalAsync
        }
    )", name.c_str()));
    test.Env.TestWaitNotification(test.Runtime, test.TxId);
    const TString path = "/MyRoot/" + name;
    WriteRow(test.Runtime, ++test.TxId, path, 0, 1, name + "-before");
    WriteRow(test.Runtime, ++test.TxId, path, 1, 2147483649u, name + "-second");
}

struct TIndexBuildExportOptions {
    bool IsFs;
    bool FinishBeforeCopy = false;
    bool Encrypted = false;
    ERestart Restart = ERestart::None;
    bool WaitForApply = false;
    bool ReadyIndex = true;
    bool RestartDuringUpload = false;
    bool FinishAfterExport = false;
};

struct TVectorRebuildExportOptions {
    bool IsFs;
    bool BeforeReplacement = false;
    bool Encrypted = false;
    bool CaptureScheme = true;
    ERestart Restart = ERestart::None;
    bool RebootDuringReplacement = false;
    bool FinishBeforeCopy = false;
    bool LegacyReadyCopy = false;
    bool RestartDuringUpload = false;
    bool RetryCopyAfterReplacement = false;
    bool FinishAfterExport = false;
    bool CorruptSnapshot = false;
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

    Y_UNIT_TEST(CopyIgnoresUnrelatedIndexBuildHistory) {
        NSchemeShard::TSchemeShard* schemeShard = nullptr;
        TExportEnv test(true, [&](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
            return schemeShard;
        });
        test.Runtime.GetAppData().FeatureFlags.SetEnableSystemNamesProtection(false);
        test.CreateTable();
        test.CreateIndex("index");
        const auto history = CreateIndexBuildHistory(test);

        for (const auto buildId : history) {
            UNIT_ASSERT(schemeShard->IndexBuilds.contains(TIndexBuildId(buildId)));
        }

        test.CreateVectorTable("Other");
        test.CreateVectorIndex("/MyRoot/Other");

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> replacement(test.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex;
        });
        const ui64 rebuildId = ++test.TxId;
        TestRebuildVectorIndex(test.Runtime, rebuildId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Other", "index", {"embedding"});
        test.Runtime.WaitFor("unrelated replacement ready", [&] { return !replacement.empty(); });
        const TString temporaryName = Sprintf("__ydb_rebuild_%lu", rebuildId);
        test.CreateIndex(temporaryName, {"value"});
        replacement.clear();
        test.Reboot();
        test.Runtime.WaitFor("unrelated replacement after restart", [&] { return !replacement.empty(); });

        for (const auto buildId : history) {
            UNIT_ASSERT(schemeShard->IndexBuilds.contains(TIndexBuildId(buildId)));
        }

        const auto targetRows = test.ReadRows("/MyRoot/Table");
        THashMap<TString, TVector<TString>> targetIndexRows;

        for (const TString& name : TVector<TString>{"index", temporaryName}) {
            targetIndexRows.emplace(name, test.ReadRows("/MyRoot/Table/" + name + "/indexImplTable"));
        }
        TestCopyTable(test.Runtime, ++test.TxId, "/MyRoot", "OrdinaryCopy", "/MyRoot/Table");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        TestDescribeResult(DescribePath(test.Runtime, "/MyRoot/OrdinaryCopy"), {NLs::IndexesCount(2)});
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/OrdinaryCopy"), targetRows);

        for (const auto& [name, rows] : targetIndexRows) {
            UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/OrdinaryCopy/" + name + "/indexImplTable"), rows);
        }
        for (const bool isBackup : {false, true}) {
            const TString dst = isBackup ? "/MyRoot/BackupCopy" : "/MyRoot/OmittedCopy";
            TestConsistentCopyTables(test.Runtime, ++test.TxId, "/MyRoot", Sprintf(R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Table"
                    DstPath: "%s"
                    OmitIndexes: true
                    IsBackup: %s
                }
            )", dst.c_str(), isBackup ? "true" : "false"));
            test.Env.TestWaitNotification(test.Runtime, test.TxId);
            TestDescribeResult(DescribePrivatePath(test.Runtime, dst), {NLs::IndexesCount(0), NLs::IsBackupTable(isBackup)});
            if (!isBackup) {
                UNIT_ASSERT_VALUES_EQUAL(test.ReadRows(dst), targetRows);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
        UNIT_ASSERT(!replacement.empty());
        TestDescribeResult(DescribePrivatePath(test.Runtime, "/MyRoot/Other/" + temporaryName), {
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        replacement.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, rebuildId);
        test.CheckBuildState(rebuildId, Ydb::Table::IndexBuildState::STATE_DONE);
    }

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
        test.Encrypted = true;
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
            UNIT_ASSERT(test.S3.GetData().contains("/Backup/export_complete.json"));
            UNIT_ASSERT(test.HasExportCompletionMarker());
        }

        test.Import(true);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored"), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsAcceptedBackupsAcrossRestarts, IsFs, LoseAcceptanceReplies) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
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

    struct TIndexBuildExportScenario {
        const TIndexBuildExportOptions Options;
        TExportEnv Test;
        ui64 BuildId = 0;
        ui64 ExportId = 0;
        ui32 ExpectedIndexes = 0;
        TVector<TString> ExpectedTableData;
        THashMap<TString, TVector<TString>> ExpectedIndexData;
        THashSet<TString> ExpectedImplTables;

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> Scans;
        TBlockEvents<TEvDataShard::TEvBuildIndexProgressResponse> Progress;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> Apply;
        TDelayedCopy Copy;
        TDelayedDataUpload Uploads;

        explicit TIndexBuildExportScenario(const TIndexBuildExportOptions& options)
            : Options(options)
            , Test(options.IsFs, &CreateFlatTxSchemeShard, TTestEnvOptions()
                .RunFakeConfigDispatcher(options.FinishAfterExport).SetupKqpProxy(options.FinishAfterExport))
            , Scans(Test.Runtime, [this](const auto& ev) { return ev->Get()->Record.GetId() == BuildId; })
            , Progress(Test.Runtime, [this](const auto& ev) { return ev->Get()->Record.GetId() == BuildId; })
            , Apply(Test.Runtime, [this](const auto& ev) {
                const auto& record = ev->Get()->Record;
                return Options.WaitForApply && record.TransactionSize() == 1
                    && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild
                    && record.GetTransaction(0).GetApplyIndexBuild().GetBuildIndexId() == BuildId;
            })
            , Copy(Test)
            , Uploads(Test)
        {
            UNIT_ASSERT(!Options.FinishAfterExport || (!Options.FinishBeforeCopy && !Options.RestartDuringUpload));
        }

        void Prepare() {
            if (Options.FinishAfterExport) {
                for (ui32 nodeIdx = 0; nodeIdx < Test.Runtime.GetNodeCount(); ++nodeIdx) {
                    Test.Runtime.GetAppData(nodeIdx).TenantName = "/MyRoot";
                }
            }

            Test.Encrypted = Options.Encrypted;
            Test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
            Test.CreateTable();
            ExpectedTableData = Test.ReadRows("/MyRoot/Table");
            if (Options.ReadyIndex) {
                const ui64 readyBuildId = Test.CreateIndex("index");
                Test.CheckBuildState(readyBuildId, Ydb::Table::IndexBuildState::STATE_DONE);
            }
        }

        void StartBuild() {
            BuildId = ++Test.TxId;
            TestBuildIndex(Test.Runtime, BuildId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "building-index", {"value"});
            Test.Runtime.WaitFor("index build scan", [&] { return !Scans.empty(); });
            TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/building-index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
            });
        }

        void StartExport() {
            ExportId = Test.Export("include_index_data: true");
            if (Options.FinishBeforeCopy) {
                Test.Runtime.WaitFor("consistent copy proposal", [&] { return !Copy.Proposal.empty(); });
                Scans.Stop().Unblock();
                Progress.Stop().Unblock();
                Apply.Stop().Unblock();
                Test.Env.TestWaitNotification(Test.Runtime, BuildId);
            }
            Copy.WaitForCopy();
            UNIT_ASSERT_VALUES_EQUAL(Test.SnapshotCount(), 1 + Options.ReadyIndex + Options.FinishBeforeCopy);
            if (Options.RestartDuringUpload) {
                UNIT_ASSERT(!Options.FinishBeforeCopy && Options.ReadyIndex && Options.Restart == ERestart::None);
                Uploads.StartAndReboot(Copy, ExportId, {"index/indexImplTable"});
            } else {
                Uploads.Requests.Stop();
            }
        }

        void RestartBuild() {
            if (!Options.FinishBeforeCopy) {
                Scans.Stop().Unblock();
                if (Options.WaitForApply) {
                    Progress.Stop().Unblock();
                    Test.Runtime.WaitFor("index apply before reboot", [&] { return !Apply.empty(); });
                } else {
                    Test.Runtime.WaitFor("index scan progress before reboot", [&] { return !Progress.empty(); });
                }
                if (Options.Restart == ERestart::DataShard || Options.Restart == ERestart::Both) {
                    Progress.clear();
                    RebootTableShards(Test, "/MyRoot/Table");
                    RebootTableShards(Test, "/MyRoot/Table/building-index/indexImplTable");
                }
                if (Options.Restart == ERestart::SchemeShard || Options.Restart == ERestart::Both) {
                    Progress.clear();
                    Apply.clear();
                    Copy.Reboot();
                }
                if (Options.Restart != ERestart::None) {
                    if (Options.WaitForApply) {
                        Test.Runtime.WaitFor("index apply after reboot", [&] { return !Apply.empty(); });
                    } else {
                        Test.Runtime.WaitFor("index scan progress after reboot", [&] { return !Progress.empty(); });
                    }
                }
                TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/building-index"), {
                    NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
                });
            }
        }

        void FinishBuild() {
            Progress.Stop().Unblock();
            Apply.Stop().Unblock();
            Test.Env.TestWaitNotification(Test.Runtime, BuildId);
            Test.CheckBuildState(BuildId, Ydb::Table::IndexBuildState::STATE_DONE);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(Test.Runtime, "/MyRoot/Table/building-index/indexImplTable"), 1);
            if (Options.ReadyIndex) {
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Table/building-index/indexImplTable"), Test.ReadRows("/MyRoot/Table/index/indexImplTable"));
            }

            const auto source = DescribePrivatePath(Test.Runtime, "/MyRoot/Table", true);
            const auto index = DescribePrivatePath(Test.Runtime, "/MyRoot/Table/building-index/indexImplTable", true);
            UNIT_ASSERT_VALUES_EQUAL(source.GetPathDescription().TablePartitionsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(index.GetPathDescription().TablePartitionsSize(), 1);
            const auto sourceData = ReadTable(Test.Runtime, source.GetPathDescription().GetTablePartitions(0).GetDatashardId(),
                "Table", {"key"}, {"key", "value"});
            const auto indexData = ReadTable(Test.Runtime, index.GetPathDescription().GetTablePartitions(0).GetDatashardId(),
                "indexImplTable", {"value", "key"}, {"key", "value"});
            UNIT_ASSERT_VALUES_EQUAL(indexData.DebugString(), sourceData.DebugString());
        }

        void CheckActiveBuild() {
            const auto result = TestGetBuildIndex(Test.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", BuildId);
            const auto state = result.GetIndexBuild().GetState();
            UNIT_ASSERT_C(state == Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA
                || state == Ydb::Table::IndexBuildState::STATE_APPLYING, result.DebugString());
            UNIT_ASSERT(Options.WaitForApply ? !Apply.empty() : !Progress.empty());
            TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/building-index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
            });
        }

        void RememberIndexData() {
            for (const TString index : {"index", "building-index"}) {
                if ((index == "index" && Options.ReadyIndex) || (index == "building-index" && Options.FinishBeforeCopy)) {
                    ExpectedIndexData.emplace(index + "/indexImplTable", Test.ReadRows("/MyRoot/Table/" + index + "/indexImplTable"));
                }
            }
        }

        void ChangeSourceData() {
            if (Options.FinishAfterExport) {
                // Use the query path so the write updates both Ready and WriteOnly indexes.
                const auto sender = Test.Runtime.AllocateEdgeActor();
                auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
                auto& request = *event->Record.MutableRequest();
                request.SetDatabase("/MyRoot");
                request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
                request.SetQuery("--!syntax_v1\nUPSERT INTO `/MyRoot/Table` (key, value) VALUES (1u, \"after\"u);");
                request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
                request.MutableTxControl()->set_commit_tx(true);
                Test.Runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(Test.Runtime.GetNodeId()), sender, event.Release()));

                const auto result = Test.Runtime.GrabEdgeEvent<NKqp::TEvKqp::TEvQueryResponse>(sender);
                UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS, result->Get()->Record.DebugString());
            } else {
                WriteRow(Test.Runtime, ++Test.TxId, "/MyRoot/Table", 0, 1, "after");
            }
        }

        void FinishExport() {
            if (Options.RestartDuringUpload) {
                Uploads.Reboot(ExportId, {"index/indexImplTable"});
                Uploads.Requests.Stop().Unblock();
            } else if (Options.FinishAfterExport) {
                CheckActiveBuild();
                Copy.Resume(false);
            } else {
                Copy.ResumeWithRebootBeforeBackup(ExportId);
            }

            Test.Env.TestWaitNotification(Test.Runtime, ExportId);
            Test.GetExportEntry(ExportId, Ydb::StatusIds::SUCCESS);
        }

        void CheckBackup() {
            const auto scheme = Test.ReadScheme();
            ExpectedIndexes = Options.ReadyIndex + Options.FinishBeforeCopy;
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), ExpectedIndexes);
            THashSet<TString> indexNames;

            for (const auto& index : scheme.indexes()) {
                indexNames.insert(index.name());
            }
            UNIT_ASSERT_VALUES_EQUAL(indexNames.contains("index"), Options.ReadyIndex);
            UNIT_ASSERT_VALUES_EQUAL(indexNames.contains("building-index"), Options.FinishBeforeCopy);
            UNIT_ASSERT_VALUES_EQUAL(Test.ReadFile("data_00.csv"), "1,\"before\"\n");
            if (Options.ReadyIndex) {
                ExpectedImplTables.insert("index/indexImplTable");
            }
            if (Options.FinishBeforeCopy) {
                ExpectedImplTables.insert("building-index/indexImplTable");
            }

            const auto prefixes = MaterializedIndexPrefixes(Test, ExpectedImplTables);

            for (const auto& [implTable, prefix] : prefixes) {
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadFile(prefix + "/data_00.csv"), "\"before\",1\n");
            }
            if (Test.Encrypted) {
                Test.CheckEncryptionIVs();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(Test.HasFile("building-index/indexImplTable/scheme.pb"), Options.FinishBeforeCopy);
            }
        }

        void CheckImport() {
            if (Options.FinishAfterExport) {
                CheckActiveBuild();
            }

            Test.Import(Test.Encrypted && !Options.FinishAfterExport);
            TestDescribeResult(DescribePath(Test.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(ExpectedIndexes)});
            UNIT_ASSERT_VALUES_EQUAL(CountRows(Test.Runtime, "/MyRoot/Restored"), 1);
            UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored"), ExpectedTableData);

            for (const auto& implTable : ExpectedImplTables) {
                UNIT_ASSERT_VALUES_EQUAL(CountRows(Test.Runtime, "/MyRoot/Restored/" + implTable), 1);
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored/" + implTable), ExpectedIndexData.at(implTable));
            }
            if (Options.FinishAfterExport) {
                CheckActiveBuild();
            }
        }

        void ForgetExport() {
            Test.Forget(ExportId);
            if (Options.FinishAfterExport) {
                CheckActiveBuild();
                FinishBuild();
            }
        }

    };

    void CheckIndexBuildExport(const TIndexBuildExportOptions& options) {
        TIndexBuildExportScenario scenario(options);
        scenario.Prepare();

        scenario.StartBuild();
        scenario.StartExport();
        scenario.RestartBuild();
        if (!options.FinishAfterExport) {
            scenario.FinishBuild();
        }

        scenario.RememberIndexData();
        scenario.ChangeSourceData();
        scenario.FinishExport();

        scenario.CheckBackup();
        scenario.CheckImport();
        scenario.ForgetExport();
    }

    Y_UNIT_TEST_QUAD(IndexBuildUsesCopiedScheme, IsFs, FinishBeforeCopy) {
        CheckIndexBuildExport({.IsFs = IsFs, .FinishBeforeCopy = FinishBeforeCopy});
    }

    Y_UNIT_TEST_QUAD(ActiveIndexBuildSurvivesRestart, IsFs, RebootDataShard) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = RebootDataShard ? ERestart::DataShard : ERestart::SchemeShard});
    }

    Y_UNIT_TEST_TWIN(IndexApplySurvivesRestartDuringExport, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::SchemeShard, .WaitForApply = true});
    }

    Y_UNIT_TEST_TWIN(UnfinishedIndexIsRemovedAfterRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::Both, .ReadyIndex = false});
    }

    Y_UNIT_TEST_TWIN(EncryptedUnfinishedIndexIsRemovedAfterRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Encrypted = true, .Restart = ERestart::Both, .ReadyIndex = false});
    }

    Y_UNIT_TEST_QUAD(EncryptedIndexBuildUsesCopiedScheme, IsFs, FinishBeforeCopy) {
        CheckIndexBuildExport({.IsFs = IsFs, .FinishBeforeCopy = FinishBeforeCopy, .Encrypted = true,
            .Restart = FinishBeforeCopy ? ERestart::None : ERestart::Both});
    }

    Y_UNIT_TEST_QUAD(ExportCompletesDuringActiveIndexBuild, IsFs, WaitForApply) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = WaitForApply ? ERestart::SchemeShard : ERestart::Both,
            .WaitForApply = WaitForApply, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(ExportCompletesWithOnlyUnfinishedIndex, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::Both, .ReadyIndex = false, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedExportCompletesDuringActiveIndexBuild, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Encrypted = true, .Restart = ERestart::Both, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(IndexBuildSurvivesExportingShardRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .RestartDuringUpload = true});
    }

    Y_UNIT_TEST_QUAD(LegacyIndexBuildUsesCopiedScheme, IsFs, IncludeIndexData) {
        TExportEnv test(IsFs);
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        test.CreateTable();
        test.CreateIndex("index");

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(test.Runtime);
        const ui64 buildId = ++test.TxId;
        TestBuildIndex(test.Runtime, buildId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "building-index", {"value"});
        test.Runtime.WaitFor("active legacy index build", [&] { return !scans.empty(); });

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export(IncludeIndexData ? "include_index_data: true" : "");
        copy.WaitForCopy(false);
        UNIT_ASSERT_VALUES_EQUAL(test.SnapshotCount(), 0);
        scans.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, buildId);
        test.CheckBuildState(buildId, Ydb::Table::IndexBuildState::STATE_DONE);

        copy.ResumeWithRebootBeforeBackup(exportId);
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        const auto scheme = test.ReadScheme();
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), IncludeIndexData ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("data_00.csv"), "1,\"before\"\n");
        if (IncludeIndexData) {
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
            UNIT_ASSERT_VALUES_EQUAL(test.ReadFile("index/indexImplTable/data_00.csv"), "\"before\",1\n");
            UNIT_ASSERT(!test.HasFile("building-index/indexImplTable/scheme.pb"));

            test.Import();
            TestDescribeResult(DescribePath(test.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(1)});
            UNIT_ASSERT_VALUES_EQUAL(CountRows(test.Runtime, "/MyRoot/Restored/index/indexImplTable"), 1);
        }

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(LegacyUserIndexWithRebuildPrefix, IsFs, Encrypted) {
        TExportEnv test(IsFs);
        test.Encrypted = Encrypted;
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        test.Runtime.GetAppData().FeatureFlags.SetEnableSystemNamesProtection(false);
        test.CreateTable();
        const TVector<TString> indexNames = {
            "__ydb_rebuild_business", "__ydb_rebuild_", "__ydb_rebuild_12_more",
            "__ydb_rebuild_18446744073709551616", "__ydb_rebuild_01",
        };
        THashMap<TString, TVector<TString>> expectedIndexData;
        THashSet<TString> expectedImplTables;

        for (const auto& name : indexNames) {
            test.CreateIndex(name, {"value"});
            expectedIndexData.emplace(name, test.ReadRows("/MyRoot/Table/" + name + "/indexImplTable"));
            expectedImplTables.insert(name + "/indexImplTable");
        }

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy(false);
        copy.ResumeWithRebootBeforeBackup(exportId);
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        const auto scheme = test.ReadScheme();
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), indexNames.size());
        const auto prefixes = MaterializedIndexPrefixes(test, expectedImplTables);

        for (const auto& name : indexNames) {
            UNIT_ASSERT_VALUES_EQUAL(test.ReadFile(prefixes.at(name + "/indexImplTable") + "/data_00.csv"), "\"before\",1\n");
        }

        test.Import(test.Encrypted);

        for (const auto& name : indexNames) {
            UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/" + name + "/indexImplTable"), expectedIndexData.at(name));
        }

        test.Forget(exportId);
    }

    void CheckLegacyChangedIndex(bool isFs, bool recreate, bool encrypted) {
        TExportEnv test(isFs);
        test.Encrypted = encrypted;
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        test.CreateTable();
        test.CreateIndex("index");
        const auto expectedData = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy(false);
        TestDropTableIndex(test.Runtime, TTestTxConfig::SchemeShard, ++test.TxId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "index"
        )");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        if (recreate) {
            test.CreateIndex("index", {"key", "value"});
            TestDescribeResult(DescribePrivatePath(test.Runtime, "/MyRoot/Table/index"), {
                NLs::IndexKeys({"key", "value"}), NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });
        }
        copy.ResumeWithRebootBeforeBackup(exportId);
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        const auto scheme = test.ReadScheme();
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).index_columns_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).index_columns(0), "value");
        const auto prefixes = MaterializedIndexPrefixes(test, {"index/indexImplTable"});
        UNIT_ASSERT_VALUES_EQUAL(test.ReadFile(prefixes.at("index/indexImplTable") + "/data_00.csv"), "\"before\",1\n");
        if (encrypted) {
            test.CheckEncryptionIVs();
        }

        test.Import(encrypted);
        TestDescribeResult(DescribePrivatePath(test.Runtime, "/MyRoot/Restored/index"), {
            NLs::IndexKeys({"value"}), NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedData);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(LegacyDroppedIndexExportsCopiedData, IsFs) {
        CheckLegacyChangedIndex(IsFs, false, false);
    }

    Y_UNIT_TEST_QUAD(LegacyRecreatedIndexUsesCopiedDefinition, IsFs, Encrypted) {
        CheckLegacyChangedIndex(IsFs, true, Encrypted);
    }

    void CheckEncryptedIndexAfterParentBackupRestart(bool isFs, bool captureScheme, bool pendingMarker = false) {
        TExportEnv test(isFs);
        test.Encrypted = true;
        test.CreateIndexedTable();

        const auto expectedIndex = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexBackups(test.Runtime, [pendingMarker](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return !pendingMarker && record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup()
                && record.GetTransaction(0).GetBackup().GetTableName() == "indexImplTable";
        });

        TBlockEvents<NSchemeShard::TEvPrivate::TEvExportCompletionUploadResult> markerReplies(test.Runtime,
            [pendingMarker](const auto& ev) { return pendingMarker && ev->Get()->ItemIdx == 1; });

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true");
        copy.WaitForCopy(captureScheme);
        copy.Resume(false);
        test.Runtime.WaitFor("pending index backup or completion marker", [&] {
            return pendingMarker ? !markerReplies.empty() : !indexBackups.empty();
        });
        bool parentDone = false;

        for (ui32 attempt = 0; attempt < 100 && !parentDone; ++attempt) {
            test.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
            parentDone = ReadExportItemState(test, exportId, 0) == NSchemeShard::TExportInfo::EState::Done;
        }
        UNIT_ASSERT(parentDone);
        const auto metadata = NBackup::TMetadata::Deserialize(test.ReadFile("metadata.json"));
        UNIT_ASSERT(metadata.GetIndexes());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->front().ExportPrefix, "001");

        TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", "Table");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        indexBackups.clear();
        indexBackups.Stop();
        markerReplies.clear();
        markerReplies.Stop();
        test.Reboot();
        test.Env.TestWaitNotification(test.Runtime, exportId);
        if (captureScheme) {
            test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
            MaterializedIndexPrefixes(test, {"index/indexImplTable"});

            test.Import(true);
            UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        } else {
            const auto result = test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "snapshot is missing");
            UNIT_ASSERT(!test.HasFile("999/scheme.pb"));
            UNIT_ASSERT(!test.HasExportCompletionMarker());
            test.Reboot();
            test.GetExportEntry(exportId, Ydb::StatusIds::CANCELLED);
        }

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(EncryptedIndexAfterParentBackupRestart, IsFs, CaptureScheme) {
        CheckEncryptedIndexAfterParentBackupRestart(IsFs, CaptureScheme);
    }

    Y_UNIT_TEST_TWIN(EncryptedIndexMarkerAfterParentBackupRestart, CaptureScheme) {
        CheckEncryptedIndexAfterParentBackupRestart(false, CaptureScheme, true);
    }

    Y_UNIT_TEST_QUAD(EncryptedMaterializedIndexesAcrossTablesAndShards, IsFs, LegacyUploader) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        ui32 oldUploaderTasks = 0;
        auto oldUploader = EmulateLegacyUploader(test, LegacyUploader, oldUploaderTasks);
        THashMap<TString, TVector<TString>> expectedData;

        for (const TString name : {"Table", "Other"}) {
            CreateShardedTableWithAsyncIndex(test, name);
            const TString path = "/MyRoot/" + name;

            for (const TString index : {"index", "other-index"}) {
                test.CreateIndex(index, index == "index" ? TVector<TString>{"value"} : TVector<TString>{"key", "value"}, path);
                expectedData.emplace(name + "/" + index, test.ReadRows(path + "/" + index + "/indexImplTable"));
            }
            expectedData.emplace(name, test.ReadRows(path));
        }

        test.CreateChangefeed();

        TDelayedCopy copy(test);

        const ui64 exportId = test.Export("include_index_data: true", {"Other"});
        copy.WaitForCopy();

        for (const TString name : {"Table", "Other"}) {
            TestDropTable(test.Runtime, ++test.TxId, "/MyRoot", name);
            test.Env.TestWaitNotification(test.Runtime, test.TxId);
        }
        copy.ResumeWithRebootBeforeBackup(exportId);
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        for (const TString table : {"backup", "Other"}) {
            const auto scheme = test.ReadScheme("scheme.pb", table);
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 3);
            MaterializedIndexPrefixes(test, {"index/indexImplTable", "other-index/indexImplTable"}, table, !LegacyUploader);
            UNIT_ASSERT(test.HasFile("data_01.csv", table));
            UNIT_ASSERT(!test.HasFile("a_async/indexImplTable/scheme.pb", table));
            const auto metadata = NBackup::TMetadata::Deserialize(test.ReadFile("metadata.json", table));
            UNIT_ASSERT(metadata.GetChangefeeds());
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetChangefeeds()->size(), table == "backup" ? 1 : 0);
            if (table == "backup") {
                const auto& feed = metadata.GetChangefeeds()->front();
                UNIT_ASSERT_VALUES_EQUAL(feed.Name, "feed");
                UNIT_ASSERT(test.HasFile(feed.ExportPrefix + "/changefeed_description.pb", table));
                UNIT_ASSERT(test.HasFile(feed.ExportPrefix + "/topic_description.pb", table));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(bool(oldUploaderTasks), LegacyUploader);
        test.CheckEncryptionIVs();

        test.Import(true, {"Other"});

        for (const TString name : {"Table", "Other"}) {
            const TString path = name == "Table" ? "/MyRoot/Restored" : "/MyRoot/RestoredOther";
            TestDescribeResult(DescribePath(test.Runtime, path), {NLs::IndexesCount(3)});
            UNIT_ASSERT_VALUES_EQUAL(test.ReadRows(path), expectedData.at(name));

            for (const TString index : {"index", "other-index"}) {
                UNIT_ASSERT_VALUES_EQUAL(test.ReadRows(path + "/" + index + "/indexImplTable"), expectedData.at(name + "/" + index));
            }
            UNIT_ASSERT_VALUES_EQUAL(CountRows(test.Runtime, path + "/a_async/indexImplTable"), 2);
        }

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(CorruptImportMetadataCancelsOnlyItsImport, IsFs) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        // Operation timestamps are persisted with second precision.
        test.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        test.CreateIndexedTable();

        const auto expectedIndex = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        const ui64 exportId = test.Export("include_index_data: true");
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        TString brokenMetadata;

        test.Import(true, {}, Ydb::StatusIds::CANCELLED, [&](ui64 importId) {
            brokenMetadata = CorruptImportMetadata(test, importId);
        });
        const ui64 failedImportId = test.TxId;
        auto checkCancelled = [&] {
            const auto entry = test.GetImportEntry(failedImportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT(entry.HasEndTime());
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "Invalid index encryption IV size");
            const auto items = ReadImportItems(test, failedImportId);
            const auto rows = NClient::TValue::Create(items)[0]["List"];
            UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["Metadata"]), brokenMetadata);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(rows[1]["ParentIndex"]), 0);
        };
        checkCancelled();
        test.Reboot();
        checkCancelled();

        // A healthy import must recover while the damaged operation is still retained.
        test.Import(true);
        const ui64 healthyImportId = test.TxId;
        UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        checkCancelled();
        TestForgetImport(test.Runtime, ++test.TxId, "/MyRoot", failedImportId);
        test.GetImportEntry(failedImportId, Ydb::StatusIds::NOT_FOUND);
        TestForgetImport(test.Runtime, ++test.TxId, "/MyRoot", healthyImportId);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptImportMetadataCancelsActiveWorkAcrossRestarts, IsFs, BuildIndex) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        test.BuildIndexesOnImport = BuildIndex;
        test.CreateIndexedTable();

        const ui64 exportId = test.Export(BuildIndex ? "" : "include_index_data: true");
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);

        TImportWork work(test, BuildIndex);
        TString brokenMetadata;

        test.Import(false, {}, Ydb::StatusIds::CANCELLED, [&](ui64 importId) {
            work.WaitForStart();

            // Also emulate a committed proposal whose acceptance reply was lost.
            brokenMetadata = CorruptImportMetadata(test, importId, true);

            for (ui32 restart = 0; restart < 2; ++restart) {
                work.ClearCancellationRequests();
                test.Reboot();
                work.WaitForCancellation();

                const auto result = test.GetImportEntry(importId);
                UNIT_ASSERT_VALUES_EQUAL(result.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_CANCELLATION);
            }
            work.Release();
        });

        const ui64 importId = test.TxId;

        const auto result = test.GetImportEntry(importId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "Invalid index encryption IV size");
        if (BuildIndex) {
            test.Env.TestWaitNotification(test.Runtime, work.BuildId);
            test.CheckBuildState(work.BuildId, Ydb::Table::IndexBuildState::STATE_CANCELLED);
        }

        test.Reboot();
        test.GetImportEntry(importId, Ydb::StatusIds::CANCELLED);
        const auto items = ReadImportItems(test, importId);
        const auto rows = NClient::TValue::Create(items)[0]["List"];
        UNIT_ASSERT_VALUES_EQUAL(rows.Size(), BuildIndex ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["Metadata"]), brokenMetadata);
        TestForgetImport(test.Runtime, ++test.TxId, "/MyRoot", importId);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(CorruptMetadataOfFinishedImportDoesNotBlockStartup, IsFs) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        test.CreateIndexedTable();

        const auto expectedIndex = test.ReadRows("/MyRoot/Table/index/indexImplTable");

        const ui64 exportId = test.Export("include_index_data: true");
        test.Env.TestWaitNotification(test.Runtime, exportId);

        test.Import();

        const ui64 importId = test.TxId;
        CorruptImportMetadata(test, importId);

        for (ui32 restart = 0; restart < 2; ++restart) {
            test.Reboot();
            test.GetImportEntry(importId, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(test.ReadRows("/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        }
        TestForgetImport(test.Runtime, ++test.TxId, "/MyRoot", importId);

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(EncryptedIndexRejectsIncorrectMetadataIV, IsFs) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        test.CreateTable();
        test.CreateIndex("index");
        ui32 corruptedTasks = 0;
        auto corruptMetadataIV = test.Runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
            if (tx.HasBackup() && !tx.GetBackup().GetEncryptionSettings().GetIndexIVs().empty()) {
                // Keep the child files valid and in this backup's namespace, but
                // advertise another item IV in authenticated root metadata.
                for (auto& [implTable, iv] : *tx.MutableBackup()->MutableEncryptionSettings()->MutableIndexIVs()) {
                    UNIT_ASSERT_VALUES_EQUAL(iv.size(), NBackup::TEncryptionIV::SIZE);
                    iv = tx.GetBackup().GetEncryptionSettings().GetIV();
                }
                record.SetTxBody(tx.SerializeAsString());
                ++corruptedTasks;
            }
        });

        const ui64 exportId = test.Export("include_index_data: true");
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GT(corruptedTasks, 0);
        const auto metadata = NBackup::TMetadata::Deserialize(test.ReadFile("metadata.json"));
        UNIT_ASSERT(metadata.GetIndexes());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
        const auto& index = metadata.GetIndexes()->front();
        UNIT_ASSERT(index.IV);
        UNIT_ASSERT_VALUES_EQUAL(index.IV->Size(), NBackup::TEncryptionIV::SIZE);
        test.ReadScheme(index.ExportPrefix + "/scheme.pb");

        test.Import(false, {}, Ydb::StatusIds::CANCELLED);

        const auto entry = test.GetImportEntry(test.TxId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_GT(entry.IssuesSize(), 0);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "File is corrupted");
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), index.ExportPrefix + "/scheme.pb.enc");

        test.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(LegacyUploaderRejectsIndexFromAnotherBackup, IsFs) {
        TExportEnv test(IsFs);
        test.Encrypted = true;
        test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        test.CreateTable();
        test.CreateIndex("index");
        ui32 oldUploaderTasks = 0;
        auto oldUploader = EmulateLegacyUploader(test, true, oldUploaderTasks);

        const ui64 exportId = test.Export("include_index_data: true");
        test.Env.TestWaitNotification(test.Runtime, exportId);
        test.GetExportEntry(exportId, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GT(oldUploaderTasks, 0);
        const auto prefixes = MaterializedIndexPrefixes(test, {"index/indexImplTable"}, "backup", false);
        const TString prefix = prefixes.at("index/indexImplTable");
        ChangeIndexBackupIV(test, prefix);

        test.Import(false, {}, Ydb::StatusIds::CANCELLED);

        const auto entry = test.GetImportEntry(test.TxId, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_GT(entry.IssuesSize(), 0);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "Index encryption IV does not belong to this backup");

        test.Forget(exportId);
    }

    struct TVectorRebuildExportScenario {
        const TVectorRebuildExportOptions Options;
        NSchemeShard::TSchemeShard* SchemeShard = nullptr;
        TExportEnv Test;
        ui64 RebuildId = 0;
        ui64 ExportId = 0;
        ui64 LiveIndexId = 0;
        TString TemporaryIndex;
        TVector<TPathId> OldImplIds;
        TVector<TString> ExpectedData;
        const TVector<TString> ImplTables = {"indexImplLevelTable", "indexImplPostingTable"};
        THashMap<TString, TVector<TString>> ExpectedIndexData;

        TBlockEvents<TEvDataShard::TEvLocalKMeansRequest> Clustering;
        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> Progress;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> Replacement;
        TDelayedCopy Copy;
        TDelayedDataUpload Uploads;

        explicit TVectorRebuildExportScenario(const TVectorRebuildExportOptions& options)
            : Options(options)
            , Test(options.IsFs, [this](const TActorId& tablet, TTabletStorageInfo* info) {
                SchemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
                return SchemeShard;
            })
            , Clustering(Test.Runtime, [this](const auto& ev) { return ev->Get()->Record.GetId() == RebuildId; })
            , Progress(Test.Runtime, [this](const auto& ev) {
                return (Options.Restart != ERestart::None || Options.FinishAfterExport) && !Options.BeforeReplacement
                    && ev->Get()->Record.GetId() == RebuildId;
            })
            , Replacement(Test.Runtime, [](const auto& ev) {
                const auto& record = ev->Get()->Record;
                return record.TransactionSize() == 1
                    && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex
                    && record.GetTransaction(0).GetMoveIndex().GetTablePath() == "/MyRoot/Table";
            })
            , Copy(Test)
            , Uploads(Test)
        {
            UNIT_ASSERT(!Options.FinishAfterExport || (!Options.FinishBeforeCopy && !Options.RestartDuringUpload
                && !Options.RebootDuringReplacement && !Options.LegacyReadyCopy));
        }

        void Prepare() {
            Test.Encrypted = Options.Encrypted;
            Test.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
            Test.CreateVectorTable();
            const ui64 buildId = Test.CreateVectorIndex();
            Test.CheckBuildState(buildId, Ydb::Table::IndexBuildState::STATE_DONE);
            WriteVectorTableRows(Test.Runtime, TTestTxConfig::SchemeShard, ++Test.TxId, "/MyRoot/Table", 0, 200, 400);

            ExpectedData = Test.ReadRows("/MyRoot/Table");

            RememberIndexData();
            LiveIndexId = DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index").GetPathId();
            if (Options.RetryCopyAfterReplacement) {
                for (const auto& implTable : ImplTables) {
                    OldImplIds.emplace_back(TTestTxConfig::SchemeShard,
                        DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index/" + implTable).GetPathId());
                }
            }
        }

        void RememberIndexData() {
            ExpectedIndexData.clear();

            for (const auto& implTable : ImplTables) {
                const TString path = "/MyRoot/Table/index/" + implTable;
                UNIT_ASSERT_GT(CountRows(Test.Runtime, path), 0);
                ExpectedIndexData.emplace(implTable, Test.ReadRows(path));
            }
        }

        void StartRebuild() {
            RebuildId = ++Test.TxId;
            TestRebuildVectorIndex(Test.Runtime, RebuildId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "index", {"embedding"});
            Test.Runtime.WaitFor("rebuild Clustering", [&] { return !Clustering.empty(); });
            if (Options.BeforeReplacement) {
                Clustering.Stop().Unblock();
                Test.Runtime.WaitFor("index replacement", [&] { return !Replacement.empty(); });
            }
            TemporaryIndex = Sprintf("__ydb_rebuild_%lu", RebuildId);
            TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/" + TemporaryIndex), {
                NLs::IndexState(Options.BeforeReplacement
                    ? NKikimrSchemeOp::EIndexStateReady : NKikimrSchemeOp::EIndexStateWriteOnly),
            });
        }

        void CheckOldLiveIndex() {
            UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index").GetPathId(), LiveIndexId);
            TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });

            for (const auto& implTable : ImplTables) {
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Table/index/" + implTable), ExpectedIndexData.at(implTable));
            }
        }

        void FinishReplacementBeforeCopy() {
            if (Options.FinishBeforeCopy) {
                Test.Runtime.WaitFor("consistent copy proposal", [&] { return !Copy.Proposal.empty(); });
                UNIT_ASSERT(Options.BeforeReplacement);
                Replacement.Stop().Unblock();
                Test.Env.TestWaitNotification(Test.Runtime, RebuildId);
                RememberIndexData();
            }
        }

        void RetryBusyCopy() {
            if (Options.RetryCopyAfterReplacement) {
                UNIT_ASSERT(Options.FinishBeforeCopy);
                Test.Runtime.WaitFor("old implementation paths cleaned after replacement", [&] {
                    return AllOf(OldImplIds, [&](const auto& id) { return !SchemeShard->PathsById.contains(id); });
                });
                const ui64 busyTxId = ++Test.TxId;

                TBlockEvents<TEvDataShard::TEvProposeTransaction> busyShards(Test.Runtime, [busyTxId](const auto& ev) {
                    return ev->Get()->Record.GetTxId() == busyTxId;
                });
                TestCopyTable(Test.Runtime, busyTxId, "/MyRoot", "BusyCopy", "/MyRoot/Table");
                Test.Runtime.WaitFor("competing copy shard proposal", [&] { return !busyShards.empty(); });
                const ui64 firstCopyTxId = Copy.Proposal.front()->Get()->Record.GetTxId();
                bool rejected = false;
                auto results = Test.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>([&](const auto& ev) {
                    const auto& record = ev->Get()->Record;
                    rejected |= record.GetTxId() == firstCopyTxId
                        && record.GetStatus() == NKikimrScheme::StatusMultipleModifications;
                });
                Copy.Proposal.Unblock(1);
                Test.Runtime.WaitFor("export copy rejected while source busy", [&] { return rejected; });
                busyShards.Stop().Unblock();
                Test.Env.TestWaitNotification(Test.Runtime, busyTxId);
                Test.Runtime.WaitFor("export retries or terminates", [&] {
                    return !Copy.Proposal.empty() || SchemeShard->Exports.at(ExportId)->IsFinished();
                });
                UNIT_ASSERT_C(!SchemeShard->Exports.at(ExportId)->IsFinished(), SchemeShard->Exports.at(ExportId)->Issue);
                UNIT_ASSERT_VALUES_UNEQUAL(Copy.Proposal.front()->Get()->Record.GetTxId(), firstCopyTxId);
            }
        }

        void CaptureCopy() {
            if (Options.LegacyReadyCopy) {
                UNIT_ASSERT(Options.BeforeReplacement && !Options.CaptureScheme);
                auto build = SchemeShard->IndexBuilds.at(TIndexBuildId(RebuildId));
                const auto rebuildName = std::exchange(build->RebuildIndexName, {});
                // Before the new selector, Ready was sufficient to Copy the private
                // Replacement. Restore the builder before any restart or MoveIndex.
                Copy.WaitForCopy(false);
                build->RebuildIndexName = rebuildName;
                TestDescribeResult(DescribePrivatePath(Test.Runtime,
                    Sprintf("/MyRoot/export-%lu/0/", ExportId) + TemporaryIndex), {
                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                });
            } else {
                Copy.WaitForCopy(Options.CaptureScheme);
            }
            UNIT_ASSERT_VALUES_EQUAL(Test.SnapshotCount(), Options.CaptureScheme ? 3 : 0);
            if (Options.CorruptSnapshot) {
                UNIT_ASSERT(Options.CaptureScheme && Options.Restart != ERestart::None);
                const auto pathId = DescribePrivatePath(Test.Runtime,
                    Sprintf("/MyRoot/export-%lu/0/index/indexImplPostingTable", ExportId)).GetPathId();
                CorruptTableSchemeSnapshot(Test, pathId);
            }
        }

        void StartUploads() {
            if (Options.RestartDuringUpload) {
                UNIT_ASSERT(Options.Restart == ERestart::None && !Options.FinishBeforeCopy);
                Uploads.StartAndReboot(Copy, ExportId, {"index/indexImplLevelTable", "index/indexImplPostingTable"});
            } else {
                Uploads.Requests.Stop();
            }
        }

        void RestartRebuild() {
            if (!Options.FinishBeforeCopy) {
                if (!Options.BeforeReplacement) {
                    Clustering.Stop().Unblock();
                    if (Options.Restart != ERestart::None) {
                        Test.Runtime.WaitFor("rebuild progress before reboot", [&] { return !Progress.empty(); });
                    }
                }
                if (Options.Restart == ERestart::DataShard || Options.Restart == ERestart::Both) {
                    Progress.clear();
                    RebootTableShards(Test, "/MyRoot/Table");

                    for (const auto& implTable : ImplTables) {
                        RebootTableShards(Test, "/MyRoot/Table/" + TemporaryIndex + "/" + implTable);
                        RebootTableShards(Test, "/MyRoot/Table/index/" + implTable);
                    }
                }
                if (Options.Restart == ERestart::SchemeShard || Options.Restart == ERestart::Both) {
                    Progress.clear();
                    Replacement.clear();
                    Copy.Reboot();
                }
                if (!Options.BeforeReplacement && Options.Restart != ERestart::None) {
                    Test.Runtime.WaitFor("rebuild progress after reboot", [&] { return !Progress.empty(); });
                }
                if (Options.BeforeReplacement) {
                    Test.Runtime.WaitFor("replacement remains blocked after reboot", [&] { return !Replacement.empty(); });
                }
                CheckOldLiveIndex();
            }
        }

        void FinishRebuild() {
            if (!Options.FinishBeforeCopy) {
                Progress.Stop().Unblock();
                Test.Runtime.WaitFor("index replacement", [&] { return !Replacement.empty(); });
                const ui64 replacementTxId = Replacement.front()->Get()->Record.GetTxId();

                TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> completion(Test.Runtime, [&](const auto& ev) {
                    return Options.RebootDuringReplacement && ev->Get()->Record.GetTxId() == replacementTxId;
                });
                Replacement.Stop().Unblock();
                if (Options.RebootDuringReplacement) {
                    Test.Runtime.WaitFor("replacement commit before reboot", [&] { return !completion.empty(); });
                    UNIT_ASSERT_VALUES_UNEQUAL(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index").GetPathId(), LiveIndexId);
                    completion.clear();
                    Copy.Reboot();
                    completion.Stop().Unblock();
                }

                Test.Env.TestWaitNotification(Test.Runtime, RebuildId);
            }

            Test.CheckBuildState(RebuildId, Ydb::Table::IndexBuildState::STATE_DONE);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(Test.Runtime, "/MyRoot/Table/index/indexImplPostingTable"), 400);
            UNIT_ASSERT_VALUES_UNEQUAL(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/index").GetPathId(), LiveIndexId);
            TestDescribeResult(DescribePrivatePath(Test.Runtime, "/MyRoot/Table/" + TemporaryIndex), {NLs::PathNotExist});
            if (Options.LegacyReadyCopy) {
                TestForgetBuildIndex(Test.Runtime, ++Test.TxId, TTestTxConfig::SchemeShard, "/MyRoot", RebuildId);
            }
        }

        void CheckActiveRebuild() {
            const auto result = TestGetBuildIndex(Test.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", RebuildId);
            const auto state = result.GetIndexBuild().GetState();
            UNIT_ASSERT_C(state == Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA
                || state == Ydb::Table::IndexBuildState::STATE_APPLYING, result.DebugString());
            UNIT_ASSERT(Options.BeforeReplacement ? !Replacement.empty() : !Progress.empty());
            CheckOldLiveIndex();
        }

        void FinishExport() {
            if (Options.CorruptSnapshot) {
                Copy.Resume(false);
            } else if (Options.RestartDuringUpload) {
                // The old live implementation has now been deleted by MoveIndex.
                Uploads.Reboot(ExportId, {"index/indexImplLevelTable", "index/indexImplPostingTable"});
                Uploads.Requests.Stop().Unblock();
            } else if (Options.FinishAfterExport) {
                CheckActiveRebuild();
                Copy.Resume(false);
            } else {
                Copy.ResumeWithRebootBeforeBackup(ExportId);
            }

            Test.Env.TestWaitNotification(Test.Runtime, ExportId);
        }

        void CheckCorruptExport() {
            const auto result = Test.GetExportEntry(ExportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues(0).message(), "table scheme snapshot");
            UNIT_ASSERT(!Test.HasExportCompletionMarker());
            UNIT_ASSERT_VALUES_EQUAL(Test.SnapshotCount(), 3);
            if (Options.FinishAfterExport) {
                CheckActiveRebuild();
            }

            Test.Forget(ExportId);
            if (Options.FinishAfterExport) {
                CheckActiveRebuild();
                FinishRebuild();
            }
        }

        void CheckHealthyBackupAfterCancellation() {
            const auto healthyRows = Test.ReadRows("/MyRoot/Table");
            RememberIndexData();
            const ui64 healthyExportId = Test.Export("include_index_data: true");
            Test.Env.TestWaitNotification(Test.Runtime, healthyExportId);
            Test.GetExportEntry(healthyExportId, Ydb::StatusIds::SUCCESS);

            Test.Import(true);
            UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored"), healthyRows);

            for (const auto& implTable : ImplTables) {
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored/index/" + implTable), ExpectedIndexData.at(implTable));
            }

            Test.Forget(healthyExportId);
        }

        void CheckRejectedLegacyExport() {
            const auto entry = Test.GetExportEntry(ExportId, Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_GT(entry.IssuesSize(), 0);
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "temporary rebuild index");

            Test.Forget(ExportId);
        }

        void CheckBackup() {
            Test.GetExportEntry(ExportId, Ydb::StatusIds::SUCCESS);

            const auto scheme = Test.ReadScheme();
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
            UNIT_ASSERT(scheme.indexes(0).has_global_vector_kmeans_tree_index());
            THashSet<TString> expectedImplTables;

            for (const auto& implTable : ImplTables) {
                expectedImplTables.insert("index/" + implTable);
                UNIT_ASSERT(!Test.HasFile(TemporaryIndex + "/" + implTable + "/scheme.pb"));
            }

            const auto prefixes = MaterializedIndexPrefixes(Test, expectedImplTables);

            for (const auto& [implTable, prefix] : prefixes) {
                UNIT_ASSERT(!Test.ReadFile(prefix + "/data_00.csv").empty());
            }
            if (Test.Encrypted) {
                Test.CheckEncryptionIVs();
            }
        }

        void CheckImport() {
            if (Options.FinishAfterExport) {
                CheckActiveRebuild();
            }

            Test.Import(Test.Encrypted && !Options.FinishAfterExport);
            TestDescribeResult(DescribePath(Test.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(1)});
            UNIT_ASSERT_VALUES_EQUAL(CountRows(Test.Runtime, "/MyRoot/Restored"), 400);
            UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored"), ExpectedData);

            for (const auto& implTable : ImplTables) {
                UNIT_ASSERT_VALUES_EQUAL(Test.ReadRows("/MyRoot/Restored/index/" + implTable), ExpectedIndexData.at(implTable));
            }
            if (Options.FinishAfterExport) {
                CheckActiveRebuild();
            }
        }

        void ForgetExport() {
            Test.Forget(ExportId);
            if (Options.FinishAfterExport) {
                CheckActiveRebuild();
                FinishRebuild();
            }
        }

        void StartExport() {
            ExportId = Test.Export("include_index_data: true");
            FinishReplacementBeforeCopy();
            RetryBusyCopy();
            CaptureCopy();
        }

        void ChangeSourceData() {
            WriteVectorTableRows(Test.Runtime, TTestTxConfig::SchemeShard, ++Test.TxId, "/MyRoot/Table", 0, 400, 401);
        }
    };

    void CheckVectorRebuildExport(const TVectorRebuildExportOptions& options) {
        TVectorRebuildExportScenario scenario(options);
        scenario.Prepare();

        scenario.StartRebuild();
        scenario.CheckOldLiveIndex();
        scenario.StartExport();
        scenario.StartUploads();
        scenario.ChangeSourceData();
        scenario.RestartRebuild();
        if (!options.FinishAfterExport) {
            scenario.FinishRebuild();
        }
        scenario.FinishExport();

        if (options.CorruptSnapshot) {
            scenario.CheckCorruptExport();
            scenario.CheckHealthyBackupAfterCancellation();
        } else if (options.LegacyReadyCopy) {
            scenario.CheckRejectedLegacyExport();
        } else {
            scenario.CheckBackup();
            scenario.CheckImport();
            scenario.ForgetExport();
        }
    }

    Y_UNIT_TEST_QUAD(VectorRebuildExportsLiveIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement});
    }

    Y_UNIT_TEST_QUAD(ActiveVectorRebuildSurvivesRestart, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Restart = ERestart::Both});
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsExportDuringVectorRebuild, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement,
            .Restart = ERestart::Both, .FinishAfterExport = true, .CorruptSnapshot = true});
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsExportAfterVectorReplacement, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Restart = ERestart::Both, .CorruptSnapshot = true});
    }

    Y_UNIT_TEST_QUAD(ExportCompletesDuringActiveVectorRebuild, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Restart = ERestart::Both, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedExportCompletesDuringActiveVectorRebuild, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .Encrypted = true, .Restart = ERestart::Both, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_QUAD(LegacyVectorRebuildExportsCopiedIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .CaptureScheme = false, .Restart = ERestart::SchemeShard});
    }

    Y_UNIT_TEST_QUAD(EncryptedVectorRebuildExportsCopiedIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Encrypted = true, .Restart = ERestart::Both});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementCommitSurvivesRestart, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .RebootDuringReplacement = true});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementBeforeCopyExportsNewIndex, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .FinishBeforeCopy = true});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementBeforeCopyRetriesBusySource, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .FinishBeforeCopy = true, .RetryCopyAfterReplacement = true});
    }

    Y_UNIT_TEST_TWIN(LegacyReadyRebuildCopyIsRejected, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .CaptureScheme = false, .Restart = ERestart::SchemeShard, .LegacyReadyCopy = true});
    }

    Y_UNIT_TEST_QUAD(VectorRebuildSurvivesExportingShardRestart, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .RestartDuringUpload = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedRebuildSurvivesExportingShardRestart, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .Encrypted = true, .RestartDuringUpload = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedIndexNamesUseTableSchemeSnapshot, ExportChangefeeds) {
        NKikimrSchemeOp::TBackupTask snapshot;
        auto& table = *snapshot.MutableTable()->MutableTable();
        auto& asyncIndex = *table.AddTableIndexes();
        asyncIndex.SetName("async-index");
        asyncIndex.SetType(NKikimrSchemeOp::EIndexTypeGlobalAsync);
        asyncIndex.AddKeyColumnNames("value");

        auto& index = *table.AddTableIndexes();
        index.SetName("index");
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        index.AddKeyColumnNames("value");

        NKikimrSchemeOp::TPathDescription changedSource(snapshot.GetTable());
        changedSource.MutableTable()->AddCdcStreams()->SetName("new-feed");

        TString error;
        auto suffix = NSchemeShard::ComputeIndexItemSuffix(&snapshot, changedSource, "index/indexImplTable", true, ExportChangefeeds, error);
        UNIT_ASSERT_C(suffix, error);
        UNIT_ASSERT_VALUES_EQUAL(*suffix, "001");
        suffix = NSchemeShard::ComputeIndexItemSuffix(nullptr, changedSource, "index/indexImplTable", true, ExportChangefeeds, error);
        UNIT_ASSERT_C(suffix, error);
        UNIT_ASSERT_VALUES_EQUAL(*suffix, ExportChangefeeds ? "002" : "001");
        suffix = NSchemeShard::ComputeIndexItemSuffix(&snapshot, changedSource, "index/indexImplTable", false, ExportChangefeeds, error);
        UNIT_ASSERT_C(suffix, error);
        UNIT_ASSERT_VALUES_EQUAL(*suffix, "index/indexImplTable");
    }

    Y_UNIT_TEST(EncryptedIndexPrefixRejectsMissingSchemeAndIndex) {
        NKikimrSchemeOp::TBackupTask snapshot;
        NKikimrSchemeOp::TPathDescription source;
        TString error;
        UNIT_ASSERT(!NSchemeShard::ComputeIndexItemSuffix(nullptr, source, "index/indexImplTable", true, true, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "missing from the table scheme");

        auto& index = *snapshot.MutableTable()->MutableTable()->AddTableIndexes();
        index.SetName("index");
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        index.AddKeyColumnNames("value");

        for (const TString path : {"other/indexImplTable", "index/other", "index"}) {
            error.clear();
            UNIT_ASSERT(!NSchemeShard::ComputeIndexItemSuffix(&snapshot, source, path, true, true, error));
            UNIT_ASSERT_STRING_CONTAINS(error, path);
        }
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
