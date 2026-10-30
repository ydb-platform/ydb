#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

enum class EIndexBackupScenario {
    RoundTrip,
    ExportOnly,
    SwapScheme,
    SwapData,
    RebootDuringImport,
    RebootBeforeScheme,
};

void ExportImportMaterializedIndexes(bool isFs, bool encrypted,
        Ydb::Import::ImportFromS3Settings::IndexPopulationMode mode,
        const TString& algorithm = "ChaCha20-Poly1305",
        EIndexBackupScenario scenario = EIndexBackupScenario::RoundTrip) {
    TTempDir tempDir;
    TPortManager portManager;
    const ui16 port = portManager.GetPort();
    TS3Mock s3Mock({}, TS3Mock::TSettings(port));
    if (!isFs) {
        UNIT_ASSERT(s3Mock.Start());
    }

    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableIndexMaterialization(true));
    runtime.GetAppData().FeatureFlags.SetEnableFsBackups(isFs);
    // Common prefixes and SchemaMapping must also work in the unencrypted control cases.
    runtime.GetAppData().FeatureFlags.SetEnableExportFiltering(true);
    runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(encrypted);
    ui64 txId = 100;

    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table"
        Columns { Name: "key" Type: "Uint32" }
        Columns { Name: "value" Type: "Utf8" }
        Columns { Name: "payload" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);
    for (ui32 row = 1; row <= 3; ++row) {
        const TString value = row < 3 ? "shared" : "other";
        const TString payload = Sprintf("payload%u", row);
        UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2, 3}, {TCell::Make(row)}, {
            TCell(value.data(), value.size()),
            TCell(payload.data(), payload.size()),
        });
    }

    // Build from actual rows: writing directly to a DataShard does not populate indexes.
    const TVector<TString> indexNames = {"by_value", "by_payload"};
    for (const auto& indexName : indexNames) {
        Ydb::Table::TableIndex index;
        index.set_name(indexName);
        index.mutable_global_index();
        index.add_index_columns(indexName == "by_value" ? "value" : "payload");
        if (indexName == "by_payload") {
            index.add_data_columns("value");
        }
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", index);
        env.TestWaitNotification(runtime, txId);
        const auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
        UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table/" + indexName + "/indexImplTable"), 3);
    }

    // Index IVs must not collide with another user item in the same export.
    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Other"
        Columns { Name: "key" Type: "Uint32" }
        Columns { Name: "value" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);
    WriteRow(runtime, ++txId, "/MyRoot/Other", 0, 1, "another table");

    const TString key = algorithm == "AES-128-GCM" ? "0123456789012345" : "Very very secret export key!!!!!";
    const TString encryptionSettings = encrypted ? Sprintf(R"(
        encryption_settings {
            encryption_algorithm: "%s"
            symmetric_key { key: "%s" }
        }
    )", algorithm.c_str(), key.c_str()) : TString();
    const TString storageSettings = isFs
        ? Sprintf("base_path: \"%s\"", tempDir.Name().c_str())
        : Sprintf("endpoint: \"localhost:%u\" scheme: HTTP", port);
    const TString backupPrefix = isFs ? "backup" : "export/backup";
    const TString exportRequest = Sprintf(R"(
        %s {
            %s
            %s
            %s
            include_index_data: true
            compression: "zstd"
            items { source_path: "/MyRoot/Table" %s: "backup" }
            items { source_path: "/MyRoot/Other" %s: "other" }
        }
    )", isFs ? "ExportToFsSettings" : "ExportToS3Settings",
        storageSettings.c_str(), encryptionSettings.c_str(),
        isFs ? "" : "destination_prefix: \"export\"", isFs ? "destination_path" : "destination_prefix",
        isFs ? "destination_path" : "destination_prefix");
    TestExport(runtime, ++txId, "/MyRoot", exportRequest);
    env.TestWaitNotification(runtime, txId);
    TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);

    const auto readFile = [&](const TString& path) -> TString {
        if (isFs) {
            return TFileInput(tempDir.Name() + "/" + path).ReadAll();
        }
        const auto* data = s3Mock.GetData().FindPtr("/" + path);
        UNIT_ASSERT_C(data, path);
        return *data;
    };
    THashSet<TString> fileIVs;
    THashMap<TString, NBackup::TEncryptionIV> metadataIVs;
    const auto readBackupFile = [&](const TString& path) {
        TString content = readFile(path + (encrypted ? ".enc" : ""));
        if (encrypted) {
            auto [plain, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                NBackup::TEncryptionKey(key), TBuffer(content.data(), content.size()));
            UNIT_ASSERT_C(fileIVs.insert(iv.GetBinaryString()).second, "Repeated IV in " << path);
            if (path.EndsWith("/metadata.json")) {
                metadataIVs.emplace(path, iv);
            }
            content.assign(plain.Data(), plain.Size());
        }
        return content;
    };
    const auto readMetadata = [&](const TString& prefix) {
        return NBackup::TMetadata::Deserialize(readBackupFile(prefix + "/metadata.json"));
    };

    const auto metadata = readMetadata(backupPrefix);
    UNIT_ASSERT(metadata.GetIndexes());
    UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), indexNames.size());
    const auto checkSchema = [&](const TString& prefix) {
        const auto content = readBackupFile(prefix + "/scheme.pb");
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(content, &scheme));
        // Authenticating the data file also checks that it has its own IV.
        UNIT_ASSERT(!readBackupFile(prefix + "/data_00.csv.zst").empty());
    };
    checkSchema(backupPrefix);
    for (const auto& index : *metadata.GetIndexes()) {
        const TString prefix = backupPrefix + "/" + index.ExportPrefix;
        checkSchema(prefix);
        readMetadata(prefix);
        if (encrypted) {
            UNIT_ASSERT_C(!index.ExportPrefix.Contains("by_"), index.ExportPrefix);
        }
        // Keep the DataShard metadata format compatible with old binaries:
        // authenticated index IVs are supplied by SchemeShard in SchemaMapping.
        UNIT_ASSERT(!index.IV);
    }

    const TString otherPrefix = isFs ? "other" : "export/other";
    readMetadata(otherPrefix);
    checkSchema(otherPrefix);

    const TString exportPrefix = isFs ? "" : "export/";
    NBackup::TSchemaMapping mapping;
    TString error;
    UNIT_ASSERT_C(mapping.Deserialize(readBackupFile(exportPrefix + "SchemaMapping/mapping.json"), error), error);
    UNIT_ASSERT_VALUES_EQUAL(mapping.Items.size(), 2);
    THashSet<TString> objectPaths;
    for (const auto& item : mapping.Items) {
        objectPaths.insert(item.ObjectPath);
        if (encrypted) {
            UNIT_ASSERT(item.IV);
            UNIT_ASSERT(*item.IV == metadataIVs.at(exportPrefix + item.ExportPrefix + "/metadata.json"));
            if (item.ObjectPath == "Table") {
                UNIT_ASSERT_VALUES_EQUAL(item.IndexIVs.size(), indexNames.size());
                for (const auto& index : *metadata.GetIndexes()) {
                    UNIT_ASSERT(item.IndexIVs.at(index.ImplTablePrefix)
                        == metadataIVs.at(backupPrefix + "/" + index.ExportPrefix + "/metadata.json"));
                }
            } else {
                UNIT_ASSERT(item.IndexIVs.empty());
            }
        } else {
            UNIT_ASSERT(item.IndexIVs.empty());
        }
    }
    UNIT_ASSERT(objectPaths.contains("Table"));
    UNIT_ASSERT(objectPaths.contains("Other"));

    if (scenario == EIndexBackupScenario::ExportOnly) {
        return;
    }

    const bool swapFiles = scenario == EIndexBackupScenario::SwapScheme || scenario == EIndexBackupScenario::SwapData;
    if (swapFiles) {
        UNIT_ASSERT(encrypted);
        const TString file = scenario == EIndexBackupScenario::SwapScheme ? "/scheme.pb.enc" : "/data_00.csv.zst.enc";
        const auto& indexes = *metadata.GetIndexes();
        const TString firstPath = backupPrefix + "/" + indexes[0].ExportPrefix + file;
        const TString secondPath = backupPrefix + "/" + indexes[1].ExportPrefix + file;
        const TString firstContent = readFile(firstPath);
        const TString secondContent = readFile(secondPath);
        const auto writeFile = [&](const TString& path, const TString& content) {
            if (isFs) {
                TFileOutput output(tempDir.Name() + "/" + path);
                output.Write(content);
                output.Finish();
            } else {
                s3Mock.GetData()["/" + path] = content;
            }
        };
        // Both ciphertexts have valid authentication tags. Import must also check
        // that each file belongs to the expected index item.
        writeFile(firstPath, secondContent);
        writeFile(secondPath, firstContent);
    }

    // Exercise both restore engines on the same backup.
    for (const bool directImport : {false, true}) {
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(directImport);
        const TString restoredPath = directImport ? "/MyRoot/RestoredDirect" : "/MyRoot/Restored";
        ui32 indexBuilds = 0;
        auto observer = runtime.AddObserver<TEvIndexBuilder::TEvCreateRequest>(
            [&](TEvIndexBuilder::TEvCreateRequest::TPtr&) { ++indexBuilds; });
        ui64 createTableTxId = 0;
        auto createObserver = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                if (ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
                    createTableTxId = ev->Get()->Record.GetTxId();
                }
            });
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> creationComplete(runtime, [&](const auto& ev) {
            return scenario == EIndexBackupScenario::RebootDuringImport
                && createTableTxId && ev->Get()->Record.GetTxId() == createTableTxId;
        });
        TBlockEvents<NKikimr::NSchemeShard::TEvPrivate::TEvImportSchemeReady> schemeReady(runtime, [&](const auto&) {
            return scenario == EIndexBackupScenario::RebootBeforeScheme;
        });
        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            %s {
                %s
                %s
                %s
                index_population_mode: %s
                items { %s: "backup" destination_path: "%s" }
            }
        )", isFs ? "ImportFromFsSettings" : "ImportFromS3Settings",
            storageSettings.c_str(), encryptionSettings.c_str(),
            isFs ? "" : "source_prefix: \"export\"",
            Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(mode).c_str(),
            isFs ? "source_path" : "source_prefix", restoredPath.c_str()));
        if (scenario == EIndexBackupScenario::RebootBeforeScheme) {
            runtime.WaitFor("scheme getter reply after persisting schema mapping", [&] { return !schemeReady.empty(); });
            UNIT_ASSERT_C(schemeReady.front()->Get()->Success, schemeReady.front()->Get()->Error);
            schemeReady.Stop().clear();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        } else if (scenario == EIndexBackupScenario::RebootDuringImport) {
            runtime.WaitFor("table creation before restoring index data", [&] { return !creationComplete.empty(); });
            // Child items and their IVs have been persisted, but no restore tasks
            // have been sent yet. Resume must recover their IVs from the database.
            creationComplete.Stop().clear();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        }
        env.TestWaitNotification(runtime, txId);
        if (swapFiles) {
            const auto result = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_C(result.DebugString().Contains("File is corrupted"), result.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(indexBuilds, 0);
            continue;
        }
        const auto result = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_C(result.GetResponse().GetEntry().GetIssues().empty(), result.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(indexBuilds,
            mode == Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD ? indexNames.size() : 0);

        TestDescribeResult(DescribePath(runtime, restoredPath), {NLs::PathExist, NLs::IndexesCount(2)});
        const auto checkData = [&](const TString& suffix) {
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, restoredPath + suffix), 3);
            UNIT_ASSERT_VALUES_EQUAL(
                ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table" + suffix),
                ReadShards(runtime, TTestTxConfig::SchemeShard, restoredPath + suffix));
        };
        checkData("");
        for (const auto& indexName : indexNames) {
            TestDescribeResult(DescribePrivatePath(runtime, restoredPath + "/" + indexName), {
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            });
            checkData("/" + indexName + "/indexImplTable");
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TEncryptedMaterializedIndexTests) {
    Y_UNIT_TEST_QUAD(Export, IsFs, Aes) {
        ExportImportMaterializedIndexes(IsFs, true,
            Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT,
            Aes ? "AES-128-GCM" : "ChaCha20-Poly1305", EIndexBackupScenario::ExportOnly);
    }

    Y_UNIT_TEST_QUAD(Build, IsFs, Encrypted) {
        ExportImportMaterializedIndexes(IsFs, Encrypted, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD);
    }

    Y_UNIT_TEST_QUAD(Import, IsFs, Encrypted) {
        ExportImportMaterializedIndexes(IsFs, Encrypted, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT);
    }

    Y_UNIT_TEST_QUAD(Auto, IsFs, Encrypted) {
        ExportImportMaterializedIndexes(IsFs, Encrypted, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO);
    }

    Y_UNIT_TEST_FLAG(ImportAes, IsFs) {
        ExportImportMaterializedIndexes(IsFs, true, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT, "AES-128-GCM");
    }

    Y_UNIT_TEST_QUAD(RejectSwappedIndexFiles, IsFs, Data) {
        ExportImportMaterializedIndexes(IsFs, true, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT,
            "ChaCha20-Poly1305", Data ? EIndexBackupScenario::SwapData : EIndexBackupScenario::SwapScheme);
    }

    Y_UNIT_TEST_FLAG(ImportWithSchemeShardReboot, IsFs) {
        ExportImportMaterializedIndexes(IsFs, true, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT,
            "ChaCha20-Poly1305", EIndexBackupScenario::RebootDuringImport);
    }

    Y_UNIT_TEST_FLAG(ImportWithRebootBeforeScheme, IsFs) {
        ExportImportMaterializedIndexes(IsFs, true, Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT,
            "ChaCha20-Poly1305", EIndexBackupScenario::RebootBeforeScheme);
    }
}
