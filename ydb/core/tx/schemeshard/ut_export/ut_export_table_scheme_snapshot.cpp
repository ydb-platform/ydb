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
            Runtime.Send(new IEventHandle(
                Runtime.GetLocalServiceId(NResourceBroker::MakeResourceBrokerID(), nodeIdx),
                sender, event.Release()), nodeIdx);
            TAutoPtr<IEventHandle> handle;
            const auto* reply = Runtime.GrabEdgeEventRethrow<
                NResourceBroker::TEvResourceBroker::TEvConfigureResult>(handle);
            UNIT_ASSERT(reply->Record.GetSuccess());
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

    void Import(bool rebootBeforeRestore = false, const TVector<TString>& extraTables = {},
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const std::function<void(ui64)>& interrupt = {}) {
        const char* indexPopulationMode = BuildIndexesOnImport ? "INDEX_POPULATION_MODE_BUILD" : "INDEX_POPULATION_MODE_IMPORT";
        TStringBuilder extraItems;
        for (const auto& name : extraTables) {
            extraItems << "items { " << (IsFs ? (Encrypted ? "source_path_db" : "source_path")
                : (Encrypted ? "source_path" : "source_prefix")) << ": \"" << name
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
            )", Directory.Path().c_str(), Encrypted ? "source_path_db" : "source_path",
                Encrypted ? "Table" : "backup", indexPopulationMode, extraItems.c_str(), EncryptionSettings().c_str())
            : Sprintf(R"(
                ImportFromS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { %s: "%s" destination_path: "/MyRoot/Restored" }
                    index_population_mode: %s
                    %s %s %s
                }
            )", Port, Encrypted ? "source_path" : "source_prefix", Encrypted ? "Table" : "backup",
                indexPopulationMode, extraItems.c_str(), EncryptionSettings().c_str(), Encrypted ? "source_prefix: \"Backup\"" : "");
        const ui64 importId = TxId + 1;
        bool schemeFailed = false;
        auto schemeResults = Runtime.AddObserver<NSchemeShard::TEvPrivate::TEvImportSchemeReady>([&](const auto& ev) {
            schemeFailed |= ev->Get()->ImportId == importId && !ev->Get()->Success;
        });
        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> allocations(Runtime, [=](const auto& ev) {
            return rebootBeforeRestore && ev->Cookie == importId;
        });
        TestImport(Runtime, ++TxId, "/MyRoot", request);
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

void CorruptTableSchemeSnapshot(TExportEnv& t, ui64 pathId, bool topics = false) {
    LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let key '(
                '('OwnerPathId (Uint64 '%lu))
                '('LocalPathId (Uint64 '%lu))
            ))
            (let value '('%s (String '"!")))
            (return (AsList (UpdateRow 'TableSchemeSnapshots key '(value))))
        )
    )", TTestTxConfig::SchemeShard, pathId, topics ? "ChangefeedUnderlyingTopics" : "TableDescription"));
}

NSchemeShard::TExportInfo::EState ReadExportItemState(TExportEnv& t, ui64 exportId, ui32 itemIdx) {
    const auto result = LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
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

TString CorruptImportMetadata(TExportEnv& t, ui64 importId, bool loseWaitTxId = false) {
    const TString metadata = R"({"indexes":[{"export_prefix":"001","impl_table_prefix":"index/indexImplTable","iv":""}]})";
    LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
        (
            (let key '('('ImportId (Uint64 '%lu)) '('Index (Uint32 '0))))
            (let metadata '('Metadata (String '"%s")))
            (return (AsList (UpdateRow 'ImportItems key '(metadata))))
        )
    )", importId, EscapeC(metadata).c_str()));
    if (loseWaitTxId) {
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('ImportId (Uint64 '%lu)) '('Index (Uint32 '0))))
                (let waitTxId '('WaitTxId (Uint64 '0)))
                (return (AsList (UpdateRow 'ImportItems key '(waitTxId))))
            )
        )", importId));
    }
    return metadata;
}

NKikimrMiniKQL::TResult ReadImportItems(TExportEnv& t, ui64 importId) {
    return LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
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
        RebootTablet(Test.Runtime, TTestTxConfig::SchemeShard, Test.Runtime.AllocateEdgeActor());
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
        RebootTablet(Test.Runtime, TTestTxConfig::SchemeShard, Test.Runtime.AllocateEdgeActor());
        allocations.Stop().Unblock();
    }
};

enum class ERestart {
    None,
    SchemeShard,
    DataShard,
    Both,
};

void RebootTableShards(TExportEnv& t, const TString& path) {
    const auto description = DescribePrivatePath(t.Runtime, path, true, true);
    const auto& partitions = description.GetPathDescription().GetTablePartitions();
    UNIT_ASSERT_C(!partitions.empty(), path);
    for (const auto& partition : partitions) {
        RebootTablet(t.Runtime, partition.GetDatashardId(), t.Runtime.AllocateEdgeActor());
    }
}

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
            THashSet<TString> keys;
            for (const auto& request : Requests) {
                keys.insert(TString(request->Get()->Request.GetKey()));
            }
            return keys.size() >= implTables.size() + 1;
        });
        THashSet<TActorId> oldUploaders;
        for (const auto& request : Requests) {
            oldUploaders.insert(request->Sender);
        }
        Requests.clear();
        const TString root = Sprintf("/MyRoot/export-%lu/0", exportId);
        RebootTableShards(Test, root);
        for (const auto& implTable : implTables) {
            RebootTableShards(Test, root + "/" + implTable);
        }
        RebootTablet(Test.Runtime, TTestTxConfig::SchemeShard, Test.Runtime.AllocateEdgeActor());
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

auto EmulateLegacyUploader(TExportEnv& t, bool enabled, ui32& oldUploaderTasks) {
    return t.Runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([enabled, &oldUploaderTasks](auto& ev) {
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

THashMap<TString, TString> MaterializedIndexPrefixes(TExportEnv& t,
        const THashSet<TString>& expectedImplTables, const TString& table = "backup", bool expectMetadataIV = true) {
    const auto metadata = NBackup::TMetadata::Deserialize(t.ReadFile("metadata.json", table));
    UNIT_ASSERT(metadata.GetIndexes());
    UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), expectedImplTables.size());
    THashMap<TString, TString> prefixes;
    for (const auto& index : *metadata.GetIndexes()) {
        UNIT_ASSERT_C(expectedImplTables.contains(index.ImplTablePrefix), index.ImplTablePrefix);
        UNIT_ASSERT(prefixes.emplace(index.ImplTablePrefix, index.ExportPrefix).second);
        UNIT_ASSERT(t.HasFile(index.ExportPrefix + "/scheme.pb", table));
        UNIT_ASSERT(t.HasFile(index.ExportPrefix + "/data_00.csv", table));
        if (t.Encrypted) {
            UNIT_ASSERT_VALUES_EQUAL(bool(index.IV), expectMetadataIV);
            if (index.IV) {
                const auto content = t.ReadRawFile(index.ExportPrefix + "/scheme.pb.enc", table);
                const auto expectedIV = NBackup::TEncryptionIV::Combine(*index.IV,
                    NBackup::EBackupFileType::TableSchema, 0, 0);
                UNIT_ASSERT_NO_EXCEPTION(NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                    NBackup::TEncryptionKey("0123456789012345"), expectedIV, TBuffer(content.data(), content.size())));
            }
        }
    }
    return prefixes;
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

enum class ESchemeShardVersionSwitch {
    None,
    Upgrade,
    Downgrade,
};

void SchemeAttributeAfterVersionSwitch(bool isFs, ESchemeShardVersionSwitch versionSwitch, const char* expectedAttribute) {
    NSchemeShard::TSchemeShard* schemeShard = nullptr;
    TExportEnv t(isFs, [&](const TActorId& tablet, TTabletStorageInfo* info) {
        schemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
        return schemeShard;
    });
    t.CreateTable();

    auto setAttribute = [&](const TString& value) {
        TestUserAttrs(t.Runtime, ++t.TxId, "/MyRoot", "Table", AlterUserAttrs({{"version_switch", value}}));
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
    };
    setAttribute("before");

    TDelayedCopy copy(t);
    const ui64 exportId = t.Export();
    copy.WaitForCopy(versionSwitch != ESchemeShardVersionSwitch::Upgrade);
    const ui64 snapshotCount = versionSwitch == ESchemeShardVersionSwitch::Upgrade ? 0 : 1;
    UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), snapshotCount);

    setAttribute("after");
    TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Table"),
        {NLs::UserAttrsEqual({{"version_switch", "after"}})});

    // Switch while the consistent copy is complete and no backup has been proposed.
    copy.Reboot();
    UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), snapshotCount);
    if (versionSwitch == ESchemeShardVersionSwitch::Downgrade) {
        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();
        // Model an old SchemeShard ignoring the additive snapshot table on startup.
        // Leave the persisted row intact; only discard the snapshot loaded in memory.
        schemeShard->Tables.at(TPathId(TTestTxConfig::SchemeShard, pathId))->BackupSettings.Clear();
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);
    }
    copy.Resume(false);
    t.Env.TestWaitNotification(t.Runtime, exportId);

    TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    Ydb::Table::CreateTableRequest scheme;
    const TString schemeText = t.ReadFile("scheme.pb");
    UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeText, &scheme), schemeText);
    UNIT_ASSERT_C(scheme.attributes().contains("version_switch"), schemeText);
    UNIT_ASSERT_VALUES_EQUAL(scheme.attributes().at("version_switch"), expectedAttribute);
    UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
    UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), snapshotCount);
    t.Forget(exportId);
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardExportTableSchemeSnapshotTests) {

    Y_UNIT_TEST(CopyIgnoresUnrelatedIndexBuildHistory) {
        NSchemeShard::TSchemeShard* schemeShard = nullptr;
        TExportEnv t(true, [&](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
            return schemeShard;
        });
        t.Runtime.GetAppData().FeatureFlags.SetEnableSystemNamesProtection(false);
        t.CreateTable();
        TestBuildIndex(t.Runtime, ++t.TxId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"value"});
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "History"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        WriteRow(t.Runtime, ++t.TxId, "/MyRoot/History", 0, 1, "history");
        TVector<ui64> history;
        for (ui32 i = 0; i < 3; ++i) {
            const ui64 buildId = ++t.TxId;
            TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
                "/MyRoot", "/MyRoot/History", "history-index", {"value"});
            t.Env.TestWaitNotification(t.Runtime, buildId);
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot", buildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            history.push_back(buildId);
            TestDropTableIndex(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot", R"(
                TableName: "History"
                IndexName: "history-index"
            )");
            t.Env.TestWaitNotification(t.Runtime, t.TxId);
        }
        for (const auto buildId : history) {
            UNIT_ASSERT(schemeShard->IndexBuilds.contains(TIndexBuildId(buildId)));
        }
        TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Other"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix" Type: "Uint32" }
            Columns { Name: "value" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        WriteVectorTableRows(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/Other", 0, 0, 200);
        TestBuildVectorIndex(t.Runtime, ++t.TxId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Other", "index", {"embedding"});
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> replacement(t.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex;
        });
        const ui64 rebuildId = ++t.TxId;
        TestRebuildVectorIndex(t.Runtime, rebuildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Other", "index", {"embedding"});
        t.Runtime.WaitFor("unrelated replacement ready", [&] { return !replacement.empty(); });
        const TString temporaryName = Sprintf("__ydb_rebuild_%lu", rebuildId);
        TestBuildIndex(t.Runtime, ++t.TxId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", temporaryName, {"value"});
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        replacement.clear();
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        t.Runtime.WaitFor("unrelated replacement after restart", [&] { return !replacement.empty(); });
        for (const auto buildId : history) {
            UNIT_ASSERT(schemeShard->IndexBuilds.contains(TIndexBuildId(buildId)));
        }
        const auto targetRows = ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        THashMap<TString, TVector<TString>> targetIndexRows;
        for (const TString& name : TVector<TString>{"index", temporaryName}) {
            targetIndexRows.emplace(name, ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Table/" + name + "/indexImplTable"));
        }
        TestCopyTable(t.Runtime, ++t.TxId, "/MyRoot", "OrdinaryCopy", "/MyRoot/Table");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/OrdinaryCopy"), {NLs::IndexesCount(2)});
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/OrdinaryCopy"), targetRows);
        for (const auto& [name, rows] : targetIndexRows) {
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/OrdinaryCopy/" + name + "/indexImplTable"), rows);
        }
        for (const bool isBackup : {false, true}) {
            const TString dst = isBackup ? "/MyRoot/BackupCopy" : "/MyRoot/OmittedCopy";
            TestConsistentCopyTables(t.Runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/Table"
                    DstPath: "%s"
                    OmitIndexes: true
                    IsBackup: %s
                }
            )", dst.c_str(), isBackup ? "true" : "false"));
            t.Env.TestWaitNotification(t.Runtime, t.TxId);
            TestDescribeResult(DescribePrivatePath(t.Runtime, dst), {NLs::IndexesCount(0), NLs::IsBackupTable(isBackup)});
            if (!isBackup) {
                UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard, dst), targetRows);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
        UNIT_ASSERT(!replacement.empty());
        TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Other/" + temporaryName), {
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        replacement.Stop().Unblock();
        t.Env.TestWaitNotification(t.Runtime, rebuildId);
        UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot", rebuildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
    }

    Y_UNIT_TEST(ExportFixturePreservesBackupQueueDefaults) {
        TExportEnv t(true);
        const auto config = NResourceBroker::MakeDefaultConfig();
        const auto queue = FindIf(config.GetQueues(), [](const auto& queue) {
            return queue.GetName() == "queue_backup";
        });
        UNIT_ASSERT(queue != config.GetQueues().end());
        for (ui32 nodeIdx = 0; nodeIdx < t.Runtime.GetNodeCount(); ++nodeIdx) {
            const auto sender = t.Runtime.AllocateEdgeActor(nodeIdx);
            t.Runtime.Send(new IEventHandle(
                t.Runtime.GetLocalServiceId(NResourceBroker::MakeResourceBrokerID(), nodeIdx), sender,
                new NResourceBroker::TEvResourceBroker::TEvConfigRequest("queue_backup")), nodeIdx);
            const auto reply = t.Runtime.GrabEdgeEvent<NResourceBroker::TEvResourceBroker::TEvConfigResponse>(sender);
            UNIT_ASSERT(reply->Get()->QueueConfig);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->QueueConfig->GetLimit().GetCpu(), queue->GetLimit().GetCpu());
        }
    }
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
                (let description '('TableDescription (String '"")))
                (let topics '('ChangefeedUnderlyingTopics (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(description topics))))
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

    Y_UNIT_TEST_QUAD(CorruptTableSchemeSnapshotCancelsOnlyItsExport, IsFs, CorruptTopics) {
        TExportEnv t(IsFs);
        // Operation timestamps are persisted with second precision.
        t.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        t.CreateTable();
        if (CorruptTopics) {
            t.CreateChangefeed();
        }

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export();
        copy.WaitForCopy();
        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();
        CorruptTableSchemeSnapshot(t, pathId, CorruptTopics);
        copy.Resume(true);
        t.Env.TestWaitNotification(t.Runtime, exportId);

        const TString error = CorruptTopics ? "changefeed topics" : "table scheme snapshot";
        auto checkCancelled = [&] {
            const auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
            const auto& entry = result.GetResponse().GetEntry();
            UNIT_ASSERT(entry.HasEndTime());
            UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), error);
            UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);
        };
        checkCancelled();
        UNIT_ASSERT(!t.HasFile("scheme.pb"));
        UNIT_ASSERT(!t.HasFile("export_complete.json"));
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        checkCancelled();

        const ui64 healthyExportId = t.Export();
        t.Env.TestWaitNotification(t.Runtime, healthyExportId);
        TestGetExport(t.Runtime, healthyExportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
        TestForgetExport(t.Runtime, ++t.TxId, "/MyRoot", healthyExportId);
        t.Env.TestWaitNotification(t.Runtime, healthyExportId);
        TestGetExport(t.Runtime, healthyExportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        checkCancelled();
        t.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptSnapshotCancelsActiveBackupsAcrossRestarts, IsFs, CorruptIndex) {
        TExportEnv t(IsFs);
        t.CreateTable(false, true);
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy();
        const TString root = Sprintf("/MyRoot/export-%lu/0", exportId);
        const TString brokenPath = CorruptIndex ? root + "/index/indexImplTable" : root;
        const auto pathId = DescribePrivatePath(t.Runtime, brokenPath).GetPathId();

        TDelayedDataUpload upload(t);
        t.ConfigureBackupConcurrency(2);
        copy.Resume(false);
        t.Runtime.WaitFor("both copied tables uploading", [&] {
            THashSet<TString> keys;
            for (const auto& request : upload.Requests) {
                keys.insert(TString(request->Get()->Request.GetKey()));
            }
            return keys.size() == 2;
        });
        CorruptTableSchemeSnapshot(t, pathId);

        TBlockEvents<TEvSchemeShard::TEvCancelTx> cancellations(t.Runtime);
        for (ui32 restart = 0; restart < 2; ++restart) {
            cancellations.clear();
            RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
            t.Runtime.WaitFor("both backup cancellations after recovery", [&] {
                THashSet<ui64> targets;
                for (const auto& request : cancellations) {
                    targets.insert(request->Get()->Record.GetTargetTxId());
                }
                return targets.size() == 2;
            });
            const auto result = TestGetExport(t.Runtime, exportId, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(result.GetResponse().GetEntry().GetProgress(),
                Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
            UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 2);
            UNIT_ASSERT(!t.HasFile("export_complete.json"));
        }
        cancellations.Stop().Unblock();
        upload.Requests.clear();
        upload.Requests.Stop();
        t.Env.TestWaitNotification(t.Runtime, exportId);
        const auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.GetResponse().GetEntry().GetIssues(0).message(), "table scheme snapshot");
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 2);
        t.Forget(exportId);
    }

    Y_UNIT_TEST(CorruptSnapshotDoesNotCancelExportAwaitingOperationMarker) {
        TExportEnv t(false);
        t.CreateTable(false, true);
        const auto expectedIndex = ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Table/index/indexImplTable");
        TBlockEvents<NSchemeShard::TEvPrivate::TEvExportCompletionUploadResult> markerReplies(t.Runtime,
            [](const auto& ev) { return ev->Get()->ItemIdx == Max<ui32>(); });
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy();
        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();
        copy.Resume(false);
        t.Runtime.WaitFor("operation completion marker uploaded", [&] { return !markerReplies.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(ReadExportItemState(t, exportId, 0), NSchemeShard::TExportInfo::EState::Done);
        UNIT_ASSERT_VALUES_EQUAL(ReadExportItemState(t, exportId, 1), NSchemeShard::TExportInfo::EState::Done);
        CorruptTableSchemeSnapshot(t, pathId);

        markerReplies.clear();
        markerReplies.Stop();
        for (ui32 restart = 0; restart < 2; ++restart) {
            RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
            t.Env.TestWaitNotification(t.Runtime, exportId);
            TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        }
        t.Import(true);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(MissingChangefeedTopicsCancelsWholeExport, IsFs) {
        TExportEnv t(IsFs);
        t.CreateTable();
        t.CreateChangefeed();

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export();
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1);
        const auto pathId = DescribePrivatePath(t.Runtime, Sprintf("/MyRoot/export-%lu/0", exportId)).GetPathId();

        // Keep the captured CDC stream, but remove its underlying topic from the snapshot.
        LocalMiniKQL(t.Runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '(
                    '('OwnerPathId (Uint64 '%lu))
                    '('LocalPathId (Uint64 '%lu))
                ))
                (let topics '('ChangefeedUnderlyingTopics (String '"")))
                (return (AsList (UpdateRow 'TableSchemeSnapshots key '(topics))))
            )
        )", TTestTxConfig::SchemeShard, pathId));
        copy.Resume(true);
        t.Env.TestWaitNotification(t.Runtime, exportId);

        auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = result.GetResponse().GetEntry();
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_VALUES_EQUAL(entry.IssuesSize(), 1);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "changefeed topics");
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

    void CheckIndexBuildExport(const TIndexBuildExportOptions& options) {
        UNIT_ASSERT(!options.FinishAfterExport || (!options.FinishBeforeCopy && !options.RestartDuringUpload));
        TExportEnv t(options.IsFs, &CreateFlatTxSchemeShard, TTestEnvOptions()
            .RunFakeConfigDispatcher(options.FinishAfterExport).SetupKqpProxy(options.FinishAfterExport));
        if (options.FinishAfterExport) {
            for (ui32 nodeIdx = 0; nodeIdx < t.Runtime.GetNodeCount(); ++nodeIdx) {
                t.Runtime.GetAppData(nodeIdx).TenantName = "/MyRoot";
            }
        }
        t.Encrypted = options.Encrypted;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.CreateTable();
        const auto expectedTableData = ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        if (options.ReadyIndex) {
            const ui64 readyBuildId = ++t.TxId;
            TestBuildIndex(t.Runtime, readyBuildId, TTestTxConfig::SchemeShard,
                "/MyRoot", "/MyRoot/Table", "index", {"value"});
            t.Env.TestWaitNotification(t.Runtime, readyBuildId);
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot", readyBuildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        }

        const ui64 buildId = ++t.TxId;
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(t.Runtime, [buildId](const auto& ev) {
            return ev->Get()->Record.GetId() == buildId;
        });
        TBlockEvents<TEvDataShard::TEvBuildIndexProgressResponse> progress(t.Runtime, [buildId](const auto& ev) {
            return ev->Get()->Record.GetId() == buildId;
        });
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> apply(t.Runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return options.WaitForApply && record.TransactionSize() == 1
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild
                && record.GetTransaction(0).GetApplyIndexBuild().GetBuildIndexId() == buildId;
        });
        TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "building-index", {"value"});
        t.Runtime.WaitFor("index build scan", [&] { return !scans.empty(); });
        TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/building-index"), {
            NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
        });

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        if (options.FinishBeforeCopy) {
            t.Runtime.WaitFor("consistent copy proposal", [&] { return !copy.Proposal.empty(); });
            scans.Stop().Unblock();
            progress.Stop().Unblock();
            apply.Stop().Unblock();
            t.Env.TestWaitNotification(t.Runtime, buildId);
        }
        copy.WaitForCopy();
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 1 + options.ReadyIndex + options.FinishBeforeCopy);
        TDelayedDataUpload uploads(t);
        if (options.RestartDuringUpload) {
            UNIT_ASSERT(!options.FinishBeforeCopy && options.ReadyIndex && options.Restart == ERestart::None);
            uploads.StartAndReboot(copy, exportId, {"index/indexImplTable"});
        } else {
            uploads.Requests.Stop();
        }

        if (!options.FinishBeforeCopy) {
            scans.Stop().Unblock();
            if (options.WaitForApply) {
                progress.Stop().Unblock();
                t.Runtime.WaitFor("index apply before reboot", [&] { return !apply.empty(); });
            } else {
                t.Runtime.WaitFor("index scan progress before reboot", [&] { return !progress.empty(); });
            }
            if (options.Restart == ERestart::DataShard || options.Restart == ERestart::Both) {
                progress.clear();
                RebootTableShards(t, "/MyRoot/Table");
                RebootTableShards(t, "/MyRoot/Table/building-index/indexImplTable");
            }
            if (options.Restart == ERestart::SchemeShard || options.Restart == ERestart::Both) {
                progress.clear();
                apply.clear();
                copy.Reboot();
            }
            if (options.Restart != ERestart::None) {
                if (options.WaitForApply) {
                    t.Runtime.WaitFor("index apply after reboot", [&] { return !apply.empty(); });
                } else {
                    t.Runtime.WaitFor("index scan progress after reboot", [&] { return !progress.empty(); });
                }
            }
            TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/building-index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
            });
        }
        auto finishBuild = [&] {
            progress.Stop().Unblock();
            apply.Stop().Unblock();
            t.Env.TestWaitNotification(t.Runtime, buildId);
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot", buildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Table/building-index/indexImplTable"), 1);
            if (options.ReadyIndex) {
                UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                    "/MyRoot/Table/building-index/indexImplTable"),
                    ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table/index/indexImplTable"));
            }
            const auto source = DescribePrivatePath(t.Runtime, "/MyRoot/Table", true);
            const auto index = DescribePrivatePath(t.Runtime, "/MyRoot/Table/building-index/indexImplTable", true);
            UNIT_ASSERT_VALUES_EQUAL(source.GetPathDescription().TablePartitionsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(index.GetPathDescription().TablePartitionsSize(), 1);
            const auto sourceData = ReadTable(t.Runtime,
                source.GetPathDescription().GetTablePartitions(0).GetDatashardId(),
                "Table", {"key"}, {"key", "value"});
            const auto indexData = ReadTable(t.Runtime,
                index.GetPathDescription().GetTablePartitions(0).GetDatashardId(),
                "indexImplTable", {"value", "key"}, {"key", "value"});
            UNIT_ASSERT_VALUES_EQUAL(indexData.DebugString(), sourceData.DebugString());
        };
        auto checkActiveBuild = [&] {
            const auto result = TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            const auto state = result.GetIndexBuild().GetState();
            UNIT_ASSERT_C(state == Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA
                || state == Ydb::Table::IndexBuildState::STATE_APPLYING, result.DebugString());
            UNIT_ASSERT(options.WaitForApply ? !apply.empty() : !progress.empty());
            TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/building-index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly),
            });
        };
        if (!options.FinishAfterExport) {
            finishBuild();
        }
        THashMap<TString, TVector<TString>> expectedIndexData;
        for (const TString index : {"index", "building-index"}) {
            if ((index == "index" && options.ReadyIndex) || (index == "building-index" && options.FinishBeforeCopy)) {
                expectedIndexData.emplace(index + "/indexImplTable", ReadShards(t.Runtime,
                    TTestTxConfig::SchemeShard, "/MyRoot/Table/" + index + "/indexImplTable"));
            }
        }
        if (options.FinishAfterExport) {
            // Use the query path so the write updates both Ready and WriteOnly indexes.
            const auto sender = t.Runtime.AllocateEdgeActor();
            auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
            auto& request = *event->Record.MutableRequest();
            request.SetDatabase("/MyRoot");
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
            request.SetQuery("--!syntax_v1\nUPSERT INTO `/MyRoot/Table` (key, value) VALUES (1u, \"after\"u);");
            request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request.MutableTxControl()->set_commit_tx(true);
            t.Runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(t.Runtime.GetNodeId()), sender, event.Release()));
            const auto result = t.Runtime.GrabEdgeEvent<NKqp::TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                result->Get()->Record.DebugString());
        } else {
            WriteRow(t.Runtime, ++t.TxId, "/MyRoot/Table", 0, 1, "after");
        }

        if (options.RestartDuringUpload) {
            uploads.Reboot(exportId, {"index/indexImplTable"});
            uploads.Requests.Stop().Unblock();
        } else if (options.FinishAfterExport) {
            checkActiveBuild();
            copy.Resume(false);
        } else {
            copy.ResumeWithRebootBeforeBackup(exportId);
        }
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));
        const ui32 expectedIndexes = options.ReadyIndex + options.FinishBeforeCopy;
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), expectedIndexes);
        THashSet<TString> indexNames;
        for (const auto& index : scheme.indexes()) {
            indexNames.insert(index.name());
        }
        UNIT_ASSERT_VALUES_EQUAL(indexNames.contains("index"), options.ReadyIndex);
        UNIT_ASSERT_VALUES_EQUAL(indexNames.contains("building-index"), options.FinishBeforeCopy);
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
        THashSet<TString> expectedImplTables;
        if (options.ReadyIndex) {
            expectedImplTables.insert("index/indexImplTable");
        }
        if (options.FinishBeforeCopy) {
            expectedImplTables.insert("building-index/indexImplTable");
        }
        const auto prefixes = MaterializedIndexPrefixes(t, expectedImplTables);
        for (const auto& [implTable, prefix] : prefixes) {
            UNIT_ASSERT_VALUES_EQUAL(t.ReadFile(prefix + "/data_00.csv"), "\"before\",1\n");
        }
        if (t.Encrypted) {
            t.CheckEncryptionIVs();
        } else {
            UNIT_ASSERT_VALUES_EQUAL(t.HasFile("building-index/indexImplTable/scheme.pb"), options.FinishBeforeCopy);
        }

        if (options.FinishAfterExport) {
            checkActiveBuild();
        }
        t.Import(t.Encrypted && !options.FinishAfterExport);
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(expectedIndexes)});
        UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Restored"), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Restored"), expectedTableData);
        for (const auto& implTable : expectedImplTables) {
            UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Restored/" + implTable), 1);
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Restored/" + implTable), expectedIndexData.at(implTable));
        }
        if (options.FinishAfterExport) {
            checkActiveBuild();
        }
        t.Forget(exportId);
        if (options.FinishAfterExport) {
            checkActiveBuild();
            finishBuild();
        }
    }

    Y_UNIT_TEST_QUAD(IndexBuildUsesCopiedScheme, IsFs, FinishBeforeCopy) {
        CheckIndexBuildExport({.IsFs = IsFs, .FinishBeforeCopy = FinishBeforeCopy});
    }

    Y_UNIT_TEST_QUAD(ActiveIndexBuildSurvivesRestart, IsFs, RebootDataShard) {
        CheckIndexBuildExport({.IsFs = IsFs,
            .Restart = RebootDataShard ? ERestart::DataShard : ERestart::SchemeShard});
    }

    Y_UNIT_TEST_TWIN(IndexApplySurvivesRestartDuringExport, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::SchemeShard, .WaitForApply = true});
    }

    Y_UNIT_TEST_TWIN(UnfinishedIndexIsRemovedAfterRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::Both, .ReadyIndex = false});
    }

    Y_UNIT_TEST_TWIN(EncryptedUnfinishedIndexIsRemovedAfterRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Encrypted = true,
            .Restart = ERestart::Both, .ReadyIndex = false});
    }

    Y_UNIT_TEST_QUAD(EncryptedIndexBuildUsesCopiedScheme, IsFs, FinishBeforeCopy) {
        CheckIndexBuildExport({.IsFs = IsFs, .FinishBeforeCopy = FinishBeforeCopy, .Encrypted = true,
            .Restart = FinishBeforeCopy ? ERestart::None : ERestart::Both});
    }

    Y_UNIT_TEST_QUAD(ExportCompletesDuringActiveIndexBuild, IsFs, WaitForApply) {
        CheckIndexBuildExport({.IsFs = IsFs,
            .Restart = WaitForApply ? ERestart::SchemeShard : ERestart::Both,
            .WaitForApply = WaitForApply, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(ExportCompletesWithOnlyUnfinishedIndex, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Restart = ERestart::Both,
            .ReadyIndex = false, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedExportCompletesDuringActiveIndexBuild, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .Encrypted = true, .Restart = ERestart::Both,
            .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(IndexBuildSurvivesExportingShardRestart, IsFs) {
        CheckIndexBuildExport({.IsFs = IsFs, .RestartDuringUpload = true});
    }

    Y_UNIT_TEST_QUAD(LegacyIndexBuildUsesCopiedScheme, IsFs, IncludeIndexData) {
        TExportEnv t(IsFs);
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.CreateTable();
        const ui64 readyBuildId = ++t.TxId;
        TestBuildIndex(t.Runtime, readyBuildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"value"});
        t.Env.TestWaitNotification(t.Runtime, readyBuildId);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(t.Runtime);
        const ui64 buildId = ++t.TxId;
        TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "building-index", {"value"});
        t.Runtime.WaitFor("active legacy index build", [&] { return !scans.empty(); });

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export(IncludeIndexData ? "include_index_data: true" : "");
        copy.WaitForCopy(false);
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), 0);
        scans.Stop().Unblock();
        t.Env.TestWaitNotification(t.Runtime, buildId);
        UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot", buildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

        copy.ResumeWithRebootBeforeBackup(exportId);
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), IncludeIndexData ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("data_00.csv"), "1,\"before\"\n");
        if (IncludeIndexData) {
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
            UNIT_ASSERT_VALUES_EQUAL(t.ReadFile("index/indexImplTable/data_00.csv"), "\"before\",1\n");
            UNIT_ASSERT(!t.HasFile("building-index/indexImplTable/scheme.pb"));
            t.Import();
            TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(1)});
            UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Restored/index/indexImplTable"), 1);
        }
        t.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(LegacyUserIndexWithRebuildPrefix, IsFs, Encrypted) {
        TExportEnv t(IsFs);
        t.Encrypted = Encrypted;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.Runtime.GetAppData().FeatureFlags.SetEnableSystemNamesProtection(false);
        t.CreateTable();
        const TVector<TString> indexNames = {
            "__ydb_rebuild_business", "__ydb_rebuild_", "__ydb_rebuild_12_more",
            "__ydb_rebuild_18446744073709551616", "__ydb_rebuild_01",
        };
        THashMap<TString, TVector<TString>> expectedIndexData;
        THashSet<TString> expectedImplTables;
        for (const auto& name : indexNames) {
            const ui64 buildId = ++t.TxId;
            TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
                "/MyRoot", "/MyRoot/Table", name, {"value"});
            t.Env.TestWaitNotification(t.Runtime, buildId);
            expectedIndexData.emplace(name, ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Table/" + name + "/indexImplTable"));
            expectedImplTables.insert(name + "/indexImplTable");
        }
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy(false);
        copy.ResumeWithRebootBeforeBackup(exportId);
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), indexNames.size());
        const auto prefixes = MaterializedIndexPrefixes(t, expectedImplTables);
        for (const auto& name : indexNames) {
            UNIT_ASSERT_VALUES_EQUAL(t.ReadFile(prefixes.at(name + "/indexImplTable") + "/data_00.csv"),
                "\"before\",1\n");
        }
        t.Import(t.Encrypted);
        for (const auto& name : indexNames) {
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Restored/" + name + "/indexImplTable"), expectedIndexData.at(name));
        }
        t.Forget(exportId);
    }

    void CheckLegacyChangedIndex(bool isFs, bool recreate, bool encrypted) {
        TExportEnv t(isFs);
        t.Encrypted = encrypted;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.CreateTable();
        const ui64 buildId = ++t.TxId;
        TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"value"});
        t.Env.TestWaitNotification(t.Runtime, buildId);
        const auto expectedData = ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table/index/indexImplTable");

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy(false);
        TestDropTableIndex(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "index"
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        if (recreate) {
            const ui64 newBuildId = ++t.TxId;
            TestBuildIndex(t.Runtime, newBuildId, TTestTxConfig::SchemeShard,
                "/MyRoot", "/MyRoot/Table", "index", {"key", "value"});
            t.Env.TestWaitNotification(t.Runtime, newBuildId);
            TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/index"), {
                NLs::IndexKeys({"key", "value"}), NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });
        }
        copy.ResumeWithRebootBeforeBackup(exportId);
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).index_columns_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).index_columns(0), "value");
        const auto prefixes = MaterializedIndexPrefixes(t, {"index/indexImplTable"});
        UNIT_ASSERT_VALUES_EQUAL(t.ReadFile(prefixes.at("index/indexImplTable") + "/data_00.csv"), "\"before\",1\n");
        if (encrypted) {
            t.CheckEncryptionIVs();
        }
        t.Import(encrypted);
        TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Restored/index"), {
            NLs::IndexKeys({"value"}), NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Restored/index/indexImplTable"), expectedData);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(LegacyDroppedIndexExportsCopiedData, IsFs) {
        CheckLegacyChangedIndex(IsFs, false, false);
    }

    Y_UNIT_TEST_QUAD(LegacyRecreatedIndexUsesCopiedDefinition, IsFs, Encrypted) {
        CheckLegacyChangedIndex(IsFs, true, Encrypted);
    }

    void CheckEncryptedIndexAfterParentBackupRestart(bool isFs, bool captureScheme, bool pendingMarker = false) {
        TExportEnv t(isFs);
        t.Encrypted = true;
        t.CreateTable(false, true);
        const auto expectedIndex = ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Table/index/indexImplTable");
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexBackups(t.Runtime, [pendingMarker](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return !pendingMarker && record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup()
                && record.GetTransaction(0).GetBackup().GetTableName() == "indexImplTable";
        });
        TBlockEvents<NSchemeShard::TEvPrivate::TEvExportCompletionUploadResult> markerReplies(t.Runtime,
            [pendingMarker](const auto& ev) { return pendingMarker && ev->Get()->ItemIdx == 1; });
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        copy.WaitForCopy(captureScheme);
        copy.Resume(false);
        t.Runtime.WaitFor("pending index backup or completion marker", [&] {
            return pendingMarker ? !markerReplies.empty() : !indexBackups.empty();
        });
        bool parentDone = false;
        for (ui32 attempt = 0; attempt < 100 && !parentDone; ++attempt) {
            t.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
            parentDone = ReadExportItemState(t, exportId, 0) == NSchemeShard::TExportInfo::EState::Done;
        }
        UNIT_ASSERT(parentDone);
        const auto metadata = NBackup::TMetadata::Deserialize(t.ReadFile("metadata.json"));
        UNIT_ASSERT(metadata.GetIndexes());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->front().ExportPrefix, "001");

        TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", "Table");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        indexBackups.clear();
        indexBackups.Stop();
        markerReplies.clear();
        markerReplies.Stop();
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        t.Env.TestWaitNotification(t.Runtime, exportId);
        if (captureScheme) {
            TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
            MaterializedIndexPrefixes(t, {"index/indexImplTable"});
            t.Import(true);
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        } else {
            const auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_STRING_CONTAINS(result.GetResponse().GetEntry().GetIssues(0).message(), "snapshot is missing");
            UNIT_ASSERT(!t.HasFile("999/scheme.pb"));
            UNIT_ASSERT(!t.HasFile("export_complete.json"));
            RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
            TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        }
        t.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(EncryptedIndexAfterParentBackupRestart, IsFs, CaptureScheme) {
        CheckEncryptedIndexAfterParentBackupRestart(IsFs, CaptureScheme);
    }

    Y_UNIT_TEST_TWIN(EncryptedIndexMarkerAfterParentBackupRestart, CaptureScheme) {
        CheckEncryptedIndexAfterParentBackupRestart(false, CaptureScheme, true);
    }

    Y_UNIT_TEST_QUAD(EncryptedMaterializedIndexesAcrossTablesAndShards, IsFs, LegacyUploader) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        ui32 oldUploaderTasks = 0;
        auto oldUploader = EmulateLegacyUploader(t, LegacyUploader, oldUploaderTasks);
        THashMap<TString, TVector<TString>> expectedData;
        for (const TString name : {"Table", "Other"}) {
            TestCreateIndexedTable(t.Runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
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
            t.Env.TestWaitNotification(t.Runtime, t.TxId);
            const TString path = "/MyRoot/" + name;
            WriteRow(t.Runtime, ++t.TxId, path, 0, 1, name + "-before");
            WriteRow(t.Runtime, ++t.TxId, path, 1, 2147483649u, name + "-second");
            for (const TString index : {"index", "other-index"}) {
                const ui64 buildId = ++t.TxId;
                TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
                    "/MyRoot", path, index, index == "index" ? TVector<TString>{"value"} : TVector<TString>{"key", "value"});
                t.Env.TestWaitNotification(t.Runtime, buildId);
                expectedData.emplace(name + "/" + index,
                    ReadShards(t.Runtime, TTestTxConfig::SchemeShard, path + "/" + index + "/indexImplTable"));
            }
            expectedData.emplace(name, ReadShards(t.Runtime, TTestTxConfig::SchemeShard, path));
        }
        t.CreateChangefeed();
        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true", {"Other"});
        copy.WaitForCopy();
        for (const TString name : {"Table", "Other"}) {
            TestDropTable(t.Runtime, ++t.TxId, "/MyRoot", name);
            t.Env.TestWaitNotification(t.Runtime, t.TxId);
        }
        copy.ResumeWithRebootBeforeBackup(exportId);
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        for (const TString table : {"backup", "Other"}) {
            Ydb::Table::CreateTableRequest scheme;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb", table), &scheme));
            UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 3);
            MaterializedIndexPrefixes(t,
                {"index/indexImplTable", "other-index/indexImplTable"}, table, !LegacyUploader);
            UNIT_ASSERT(t.HasFile("data_01.csv", table));
            UNIT_ASSERT(!t.HasFile("a_async/indexImplTable/scheme.pb", table));
            const auto metadata = NBackup::TMetadata::Deserialize(t.ReadFile("metadata.json", table));
            UNIT_ASSERT(metadata.GetChangefeeds());
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetChangefeeds()->size(), table == "backup" ? 1 : 0);
            if (table == "backup") {
                const auto& feed = metadata.GetChangefeeds()->front();
                UNIT_ASSERT_VALUES_EQUAL(feed.Name, "feed");
                UNIT_ASSERT(t.HasFile(feed.ExportPrefix + "/changefeed_description.pb", table));
                UNIT_ASSERT(t.HasFile(feed.ExportPrefix + "/topic_description.pb", table));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(bool(oldUploaderTasks), LegacyUploader);
        t.CheckEncryptionIVs();
        t.Import(true, {"Other"});
        for (const TString name : {"Table", "Other"}) {
            const TString path = name == "Table" ? "/MyRoot/Restored" : "/MyRoot/RestoredOther";
            TestDescribeResult(DescribePath(t.Runtime, path), {NLs::IndexesCount(3)});
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard, path), expectedData.at(name));
            for (const TString index : {"index", "other-index"}) {
                UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                    path + "/" + index + "/indexImplTable"), expectedData.at(name + "/" + index));
            }
            UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, path + "/a_async/indexImplTable"), 2);
        }
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(CorruptImportMetadataCancelsOnlyItsImport, IsFs) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        // Operation timestamps are persisted with second precision.
        t.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        t.CreateTable(false, true);
        const auto expectedIndex = ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Table/index/indexImplTable");
        const ui64 exportId = t.Export("include_index_data: true");
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        TString brokenMetadata;
        t.Import(true, {}, Ydb::StatusIds::CANCELLED, [&](ui64 importId) {
            brokenMetadata = CorruptImportMetadata(t, importId);
        });
        const ui64 failedImportId = t.TxId;
        auto checkCancelled = [&] {
            const auto result = TestGetImport(t.Runtime, failedImportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
            const auto& entry = result.GetResponse().GetEntry();
            UNIT_ASSERT(entry.HasEndTime());
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "Invalid index encryption IV size");
            const auto items = ReadImportItems(t, failedImportId);
            const auto rows = NClient::TValue::Create(items)[0]["List"];
            UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["Metadata"]), brokenMetadata);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(rows[1]["ParentIndex"]), 0);
        };
        checkCancelled();
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        checkCancelled();

        // A healthy import must recover while the damaged operation is still retained.
        t.Import(true);
        const ui64 healthyImportId = t.TxId;
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        checkCancelled();
        TestForgetImport(t.Runtime, ++t.TxId, "/MyRoot", failedImportId);
        TestGetImport(t.Runtime, failedImportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        TestForgetImport(t.Runtime, ++t.TxId, "/MyRoot", healthyImportId);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_QUAD(CorruptImportMetadataCancelsActiveWorkAcrossRestarts, IsFs, BuildIndex) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        t.BuildIndexesOnImport = BuildIndex;
        t.CreateTable(false, true);
        const ui64 exportId = t.Export(BuildIndex ? "" : "include_index_data: true");
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        TBlockEvents<NWrappers::TEvExternalStorage::TEvGetObjectRequest> dataReads(t.Runtime, [](const auto& ev) {
            const TString key(ev->Get()->Request.GetKey());
            return !BuildIndex && (key.EndsWith(".csv") || key.EndsWith(".csv.enc"));
        });
        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> scans(t.Runtime, [](const auto&) { return BuildIndex; });
        TBlockEvents<TEvSchemeShard::TEvCancelTx> restoreCancellations(t.Runtime);
        TBlockEvents<TEvIndexBuilder::TEvCancelRequest> buildCancellations(t.Runtime);
        ui64 buildId = 0;
        TString brokenMetadata;
        t.Import(false, {}, Ydb::StatusIds::CANCELLED, [&](ui64 importId) {
            t.Runtime.WaitFor("active import work", [&] {
                if (BuildIndex) {
                    return !scans.empty();
                }
                THashSet<TString> keys;
                for (const auto& request : dataReads) {
                    keys.insert(TString(request->Get()->Request.GetKey()));
                }
                return keys.size() == 2;
            });
            if (BuildIndex) {
                buildId = scans.front()->Get()->Record.GetId();
            }
            // Also emulate a committed proposal whose acceptance reply was lost.
            brokenMetadata = CorruptImportMetadata(t, importId, true);
            for (ui32 restart = 0; restart < 2; ++restart) {
                restoreCancellations.clear();
                buildCancellations.clear();
                RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
                t.Runtime.WaitFor("active import cancellation after recovery", [&] {
                    if (BuildIndex) {
                        return !buildCancellations.empty();
                    }
                    THashSet<ui64> targets;
                    for (const auto& request : restoreCancellations) {
                        targets.insert(request->Get()->Record.GetTargetTxId());
                    }
                    return targets.size() == 2;
                });
                const auto result = TestGetImport(t.Runtime, importId, "/MyRoot");
                UNIT_ASSERT_VALUES_EQUAL(result.GetResponse().GetEntry().GetProgress(),
                    Ydb::Import::ImportProgress::PROGRESS_CANCELLATION);
            }
            restoreCancellations.Stop().Unblock();
            buildCancellations.Stop().Unblock();
            dataReads.clear();
            dataReads.Stop();
            scans.clear();
            scans.Stop();
        });
        const ui64 importId = t.TxId;
        const auto result = TestGetImport(t.Runtime, importId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.GetResponse().GetEntry().GetIssues(0).message(), "Invalid index encryption IV size");
        if (BuildIndex) {
            t.Env.TestWaitNotification(t.Runtime, buildId);
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot", buildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);
        }
        RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
        TestGetImport(t.Runtime, importId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto items = ReadImportItems(t, importId);
        const auto rows = NClient::TValue::Create(items)[0]["List"];
        UNIT_ASSERT_VALUES_EQUAL(rows.Size(), BuildIndex ? 1 : 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["Metadata"]), brokenMetadata);
        TestForgetImport(t.Runtime, ++t.TxId, "/MyRoot", importId);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(CorruptMetadataOfFinishedImportDoesNotBlockStartup, IsFs) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        t.CreateTable(false, true);
        const auto expectedIndex = ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/Table/index/indexImplTable");
        const ui64 exportId = t.Export("include_index_data: true");
        t.Env.TestWaitNotification(t.Runtime, exportId);
        t.Import();
        const ui64 importId = t.TxId;
        CorruptImportMetadata(t, importId);
        for (ui32 restart = 0; restart < 2; ++restart) {
            RebootTablet(t.Runtime, TTestTxConfig::SchemeShard, t.Runtime.AllocateEdgeActor());
            TestGetImport(t.Runtime, importId, "/MyRoot", Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Restored/index/indexImplTable"), expectedIndex);
        }
        TestForgetImport(t.Runtime, ++t.TxId, "/MyRoot", importId);
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(EncryptedIndexRejectsIncorrectMetadataIV, IsFs) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.CreateTable();
        const ui64 buildId = ++t.TxId;
        TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"value"});
        t.Env.TestWaitNotification(t.Runtime, buildId);
        ui32 corruptedTasks = 0;
        auto corruptMetadataIV = t.Runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
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
        const ui64 exportId = t.Export("include_index_data: true");
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GT(corruptedTasks, 0);
        const auto metadata = NBackup::TMetadata::Deserialize(t.ReadFile("metadata.json"));
        UNIT_ASSERT(metadata.GetIndexes());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
        const auto& index = metadata.GetIndexes()->front();
        UNIT_ASSERT(index.IV);
        UNIT_ASSERT_VALUES_EQUAL(index.IV->Size(), NBackup::TEncryptionIV::SIZE);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile(index.ExportPrefix + "/scheme.pb"), &scheme));
        t.Import(false, {}, Ydb::StatusIds::CANCELLED);
        const auto result = TestGetImport(t.Runtime, t.TxId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = result.GetResponse().GetEntry();
        UNIT_ASSERT_GT(entry.IssuesSize(), 0);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "File is corrupted");
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), index.ExportPrefix + "/scheme.pb.enc");
        t.Forget(exportId);
    }

    Y_UNIT_TEST_TWIN(LegacyUploaderRejectsIndexFromAnotherBackup, IsFs) {
        TExportEnv t(IsFs);
        t.Encrypted = true;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        t.CreateTable();
        const ui64 buildId = ++t.TxId;
        TestBuildIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"value"});
        t.Env.TestWaitNotification(t.Runtime, buildId);
        ui32 oldUploaderTasks = 0;
        auto oldUploader = EmulateLegacyUploader(t, true, oldUploaderTasks);
        const ui64 exportId = t.Export("include_index_data: true");
        t.Env.TestWaitNotification(t.Runtime, exportId);
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_GT(oldUploaderTasks, 0);
        const auto prefixes = MaterializedIndexPrefixes(t, {"index/indexImplTable"}, "backup", false);
        const TString prefix = prefixes.at("index/indexImplTable");
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
        if (IsFs) {
            TVector<TFsPath> files;
            (t.Directory.Path() / "backup" / prefix).List(files);
            for (const auto& file : files) {
                if (file.GetPath().EndsWith(".enc")) {
                    const auto content = changeBackupIV(TFileInput(file).ReadAll());
                    TFileOutput(file).Write(content);
                }
            }
        } else {
            for (auto& [name, content] : t.S3.GetData()) {
                if (name.StartsWith("/Backup/backup/" + prefix + "/") && name.EndsWith(".enc")) {
                    content = changeBackupIV(content);
                }
            }
        }
        t.Import(false, {}, Ydb::StatusIds::CANCELLED);
        const auto result = TestGetImport(t.Runtime, t.TxId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = result.GetResponse().GetEntry();
        UNIT_ASSERT_GT(entry.IssuesSize(), 0);
        UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "Index encryption IV does not belong to this backup");
        t.Forget(exportId);
    }

    void CheckVectorRebuildExport(const TVectorRebuildExportOptions& options) {
        UNIT_ASSERT(!options.FinishAfterExport || (!options.FinishBeforeCopy && !options.RestartDuringUpload
            && !options.RebootDuringReplacement && !options.LegacyReadyCopy));
        NSchemeShard::TSchemeShard* schemeShard = nullptr;
        TExportEnv t(options.IsFs, [&](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeShard = static_cast<NSchemeShard::TSchemeShard*>(CreateFlatTxSchemeShard(tablet, info));
            return schemeShard;
        });
        t.Encrypted = options.Encrypted;
        t.Runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);
        TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix" Type: "Uint32" }
            Columns { Name: "value" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        WriteVectorTableRows(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/Table", 0, 0, 200);
        const ui64 buildId = ++t.TxId;
        TestBuildVectorIndex(t.Runtime, buildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"embedding"});
        t.Env.TestWaitNotification(t.Runtime, buildId);
        UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
            "/MyRoot", buildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        WriteVectorTableRows(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/Table", 0, 200, 400);

        const auto expectedData = ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        const TVector<TString> implTables = {"indexImplLevelTable", "indexImplPostingTable"};
        THashMap<TString, TVector<TString>> expectedIndexData;
        auto rememberIndexData = [&] {
            expectedIndexData.clear();
            for (const auto& implTable : implTables) {
                const TString path = "/MyRoot/Table/index/" + implTable;
                UNIT_ASSERT_GT(CountRows(t.Runtime, path), 0);
                expectedIndexData.emplace(implTable, ReadShards(t.Runtime, TTestTxConfig::SchemeShard, path));
            }
        };
        rememberIndexData();
        const ui64 liveIndexId = DescribePrivatePath(t.Runtime, "/MyRoot/Table/index").GetPathId();
        TVector<TPathId> oldImplIds;
        if (options.RetryCopyAfterReplacement) {
            for (const auto& implTable : implTables) {
                oldImplIds.emplace_back(TTestTxConfig::SchemeShard,
                    DescribePrivatePath(t.Runtime, "/MyRoot/Table/index/" + implTable).GetPathId());
            }
        }

        const ui64 rebuildId = ++t.TxId;
        TBlockEvents<TEvDataShard::TEvLocalKMeansRequest> clustering(t.Runtime, [rebuildId](const auto& ev) {
            return ev->Get()->Record.GetId() == rebuildId;
        });
        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> progress(t.Runtime, [&](const auto& ev) {
            return (options.Restart != ERestart::None || options.FinishAfterExport) && !options.BeforeReplacement
                && ev->Get()->Record.GetId() == rebuildId;
        });
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> replacement(t.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1
                && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex
                && record.GetTransaction(0).GetMoveIndex().GetTablePath() == "/MyRoot/Table";
        });
        TestRebuildVectorIndex(t.Runtime, rebuildId, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index", {"embedding"});
        t.Runtime.WaitFor("rebuild clustering", [&] { return !clustering.empty(); });
        if (options.BeforeReplacement) {
            clustering.Stop().Unblock();
            t.Runtime.WaitFor("index replacement", [&] { return !replacement.empty(); });
        }
        const TString temporaryIndex = Sprintf("__ydb_rebuild_%lu", rebuildId);
        TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/" + temporaryIndex), {
            NLs::IndexState(options.BeforeReplacement
                ? NKikimrSchemeOp::EIndexStateReady : NKikimrSchemeOp::EIndexStateWriteOnly),
        });
        auto checkOldLiveIndex = [&] {
            UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(t.Runtime, "/MyRoot/Table/index").GetPathId(), liveIndexId);
            TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/index"), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });
            for (const auto& implTable : implTables) {
                UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                    "/MyRoot/Table/index/" + implTable), expectedIndexData.at(implTable));
            }
        };
        checkOldLiveIndex();

        TDelayedCopy copy(t);
        const ui64 exportId = t.Export("include_index_data: true");
        if (options.FinishBeforeCopy) {
            t.Runtime.WaitFor("consistent copy proposal", [&] { return !copy.Proposal.empty(); });
            UNIT_ASSERT(options.BeforeReplacement);
            replacement.Stop().Unblock();
            t.Env.TestWaitNotification(t.Runtime, rebuildId);
            rememberIndexData();
        }
        if (options.RetryCopyAfterReplacement) {
            UNIT_ASSERT(options.FinishBeforeCopy);
            t.Runtime.WaitFor("old implementation paths cleaned after replacement", [&] {
                return AllOf(oldImplIds, [&](const auto& id) { return !schemeShard->PathsById.contains(id); });
            });
            const ui64 busyTxId = ++t.TxId;
            TBlockEvents<TEvDataShard::TEvProposeTransaction> busyShards(t.Runtime, [busyTxId](const auto& ev) {
                return ev->Get()->Record.GetTxId() == busyTxId;
            });
            TestCopyTable(t.Runtime, busyTxId, "/MyRoot", "BusyCopy", "/MyRoot/Table");
            t.Runtime.WaitFor("competing copy shard proposal", [&] { return !busyShards.empty(); });
            const ui64 firstCopyTxId = copy.Proposal.front()->Get()->Record.GetTxId();
            bool rejected = false;
            auto results = t.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>([&](const auto& ev) {
                const auto& record = ev->Get()->Record;
                rejected |= record.GetTxId() == firstCopyTxId
                    && record.GetStatus() == NKikimrScheme::StatusMultipleModifications;
            });
            copy.Proposal.Unblock(1);
            t.Runtime.WaitFor("export copy rejected while source busy", [&] { return rejected; });
            busyShards.Stop().Unblock();
            t.Env.TestWaitNotification(t.Runtime, busyTxId);
            t.Runtime.WaitFor("export retries or terminates", [&] {
                return !copy.Proposal.empty() || schemeShard->Exports.at(exportId)->IsFinished();
            });
            UNIT_ASSERT_C(!schemeShard->Exports.at(exportId)->IsFinished(), schemeShard->Exports.at(exportId)->Issue);
            UNIT_ASSERT_VALUES_UNEQUAL(copy.Proposal.front()->Get()->Record.GetTxId(), firstCopyTxId);
        }
        if (options.LegacyReadyCopy) {
            UNIT_ASSERT(options.BeforeReplacement && !options.CaptureScheme);
            auto build = schemeShard->IndexBuilds.at(TIndexBuildId(rebuildId));
            const auto rebuildName = std::exchange(build->RebuildIndexName, {});
            // Before the new selector, Ready was sufficient to copy the private
            // replacement. Restore the builder before any restart or MoveIndex.
            copy.WaitForCopy(false);
            build->RebuildIndexName = rebuildName;
            TestDescribeResult(DescribePrivatePath(t.Runtime,
                Sprintf("/MyRoot/export-%lu/0/", exportId) + temporaryIndex), {
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });
        } else {
            copy.WaitForCopy(options.CaptureScheme);
        }
        UNIT_ASSERT_VALUES_EQUAL(t.SnapshotCount(), options.CaptureScheme ? 3 : 0);
        TDelayedDataUpload uploads(t);
        if (options.RestartDuringUpload) {
            UNIT_ASSERT(options.Restart == ERestart::None && !options.FinishBeforeCopy);
            uploads.StartAndReboot(copy, exportId, {"index/indexImplLevelTable", "index/indexImplPostingTable"});
        } else {
            uploads.Requests.Stop();
        }

        WriteVectorTableRows(t.Runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/Table", 0, 400, 401);
        if (!options.FinishBeforeCopy) {
            if (!options.BeforeReplacement) {
                clustering.Stop().Unblock();
                if (options.Restart != ERestart::None) {
                    t.Runtime.WaitFor("rebuild progress before reboot", [&] { return !progress.empty(); });
                }
            }
            if (options.Restart == ERestart::DataShard || options.Restart == ERestart::Both) {
                progress.clear();
                RebootTableShards(t, "/MyRoot/Table");
                for (const auto& implTable : implTables) {
                    RebootTableShards(t, "/MyRoot/Table/" + temporaryIndex + "/" + implTable);
                    RebootTableShards(t, "/MyRoot/Table/index/" + implTable);
                }
            }
            if (options.Restart == ERestart::SchemeShard || options.Restart == ERestart::Both) {
                progress.clear();
                replacement.clear();
                copy.Reboot();
            }
            if (!options.BeforeReplacement && options.Restart != ERestart::None) {
                t.Runtime.WaitFor("rebuild progress after reboot", [&] { return !progress.empty(); });
            }
            if (options.BeforeReplacement) {
                t.Runtime.WaitFor("replacement remains blocked after reboot", [&] { return !replacement.empty(); });
            }
            checkOldLiveIndex();
        }
        auto finishRebuild = [&] {
            if (!options.FinishBeforeCopy) {
                progress.Stop().Unblock();
                t.Runtime.WaitFor("index replacement", [&] { return !replacement.empty(); });
                const ui64 replacementTxId = replacement.front()->Get()->Record.GetTxId();
                TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> completion(t.Runtime, [&](const auto& ev) {
                    return options.RebootDuringReplacement && ev->Get()->Record.GetTxId() == replacementTxId;
                });
                replacement.Stop().Unblock();
                if (options.RebootDuringReplacement) {
                    t.Runtime.WaitFor("replacement commit before reboot", [&] { return !completion.empty(); });
                    UNIT_ASSERT_VALUES_UNEQUAL(DescribePrivatePath(t.Runtime, "/MyRoot/Table/index").GetPathId(), liveIndexId);
                    completion.clear();
                    copy.Reboot();
                    completion.Stop().Unblock();
                }
                t.Env.TestWaitNotification(t.Runtime, rebuildId);
            }
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot", rebuildId).GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Table/index/indexImplPostingTable"), 400);
            UNIT_ASSERT_VALUES_UNEQUAL(DescribePrivatePath(t.Runtime, "/MyRoot/Table/index").GetPathId(), liveIndexId);
            TestDescribeResult(DescribePrivatePath(t.Runtime, "/MyRoot/Table/" + temporaryIndex), {NLs::PathNotExist});
            if (options.LegacyReadyCopy) {
                TestForgetBuildIndex(t.Runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", rebuildId);
            }
        };
        auto checkActiveRebuild = [&] {
            const auto result = TestGetBuildIndex(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", rebuildId);
            const auto state = result.GetIndexBuild().GetState();
            UNIT_ASSERT_C(state == Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA
                || state == Ydb::Table::IndexBuildState::STATE_APPLYING, result.DebugString());
            UNIT_ASSERT(options.BeforeReplacement ? !replacement.empty() : !progress.empty());
            checkOldLiveIndex();
        };
        if (!options.FinishAfterExport) {
            finishRebuild();
        }

        if (options.RestartDuringUpload) {
            // The old live implementation has now been deleted by MoveIndex.
            uploads.Reboot(exportId, {"index/indexImplLevelTable", "index/indexImplPostingTable"});
            uploads.Requests.Stop().Unblock();
        } else if (options.FinishAfterExport) {
            checkActiveRebuild();
            copy.Resume(false);
        } else {
            copy.ResumeWithRebootBeforeBackup(exportId);
        }
        t.Env.TestWaitNotification(t.Runtime, exportId);
        if (options.LegacyReadyCopy) {
            const auto result = TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
            const auto& entry = result.GetResponse().GetEntry();
            UNIT_ASSERT_GT(entry.IssuesSize(), 0);
            UNIT_ASSERT_STRING_CONTAINS(entry.GetIssues(0).message(), "temporary rebuild index");
            t.Forget(exportId);
            return;
        }
        TestGetExport(t.Runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(t.ReadFile("scheme.pb"), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheme.indexes(0).name(), "index");
        UNIT_ASSERT(scheme.indexes(0).has_global_vector_kmeans_tree_index());
        THashSet<TString> expectedImplTables;
        for (const auto& implTable : implTables) {
            expectedImplTables.insert("index/" + implTable);
            UNIT_ASSERT(!t.HasFile(temporaryIndex + "/" + implTable + "/scheme.pb"));
        }
        const auto prefixes = MaterializedIndexPrefixes(t, expectedImplTables);
        for (const auto& [implTable, prefix] : prefixes) {
            UNIT_ASSERT(!t.ReadFile(prefix + "/data_00.csv").empty());
        }
        if (t.Encrypted) {
            t.CheckEncryptionIVs();
        }

        if (options.FinishAfterExport) {
            checkActiveRebuild();
        }
        t.Import(t.Encrypted && !options.FinishAfterExport);
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Restored"), {NLs::IndexesCount(1)});
        UNIT_ASSERT_VALUES_EQUAL(CountRows(t.Runtime, "/MyRoot/Restored"), 400);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard, "/MyRoot/Restored"), expectedData);
        for (const auto& implTable : implTables) {
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(t.Runtime, TTestTxConfig::SchemeShard,
                "/MyRoot/Restored/index/" + implTable), expectedIndexData.at(implTable));
        }
        if (options.FinishAfterExport) {
            checkActiveRebuild();
        }
        t.Forget(exportId);
        if (options.FinishAfterExport) {
            checkActiveRebuild();
            finishRebuild();
        }
    }

    Y_UNIT_TEST_QUAD(VectorRebuildExportsLiveIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement});
    }

    Y_UNIT_TEST_QUAD(ActiveVectorRebuildSurvivesRestart, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Restart = ERestart::Both});
    }

    Y_UNIT_TEST_QUAD(ExportCompletesDuringActiveVectorRebuild, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement,
            .Restart = ERestart::Both, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_TWIN(EncryptedExportCompletesDuringActiveVectorRebuild, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .Encrypted = true,
            .Restart = ERestart::Both, .FinishAfterExport = true});
    }

    Y_UNIT_TEST_QUAD(LegacyVectorRebuildExportsCopiedIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .CaptureScheme = false,
            .Restart = ERestart::SchemeShard});
    }

    Y_UNIT_TEST_QUAD(EncryptedVectorRebuildExportsCopiedIndex, IsFs, BeforeReplacement) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = BeforeReplacement, .Encrypted = true,
            .Restart = ERestart::Both});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementCommitSurvivesRestart, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .RebootDuringReplacement = true});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementBeforeCopyExportsNewIndex, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .FinishBeforeCopy = true});
    }

    Y_UNIT_TEST_TWIN(VectorReplacementBeforeCopyRetriesBusySource, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .FinishBeforeCopy = true,
            .RetryCopyAfterReplacement = true});
    }

    Y_UNIT_TEST_TWIN(LegacyReadyRebuildCopyIsRejected, IsFs) {
        CheckVectorRebuildExport({.IsFs = IsFs, .BeforeReplacement = true, .CaptureScheme = false,
            .Restart = ERestart::SchemeShard, .LegacyReadyCopy = true});
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
