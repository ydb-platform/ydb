#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

class TBackupStorage {
public:
    explicit TBackupStorage(bool isFs)
        : IsFs(isFs)
        , Port(Ports.GetPort())
        , S3({}, TS3Mock::TSettings(Port))
    {
        if (!IsFs) {
            UNIT_ASSERT(S3.Start());
        }
    }

    TString Settings() const {
        return IsFs ? Sprintf("base_path: \"%s\"", Dir.Name().c_str())
            : Sprintf("endpoint: \"localhost:%u\" scheme: HTTP", Port);
    }

    TString Read(const TString& path) {
        if (IsFs) {
            return TFileInput(Dir.Name() + "/" + path).ReadAll();
        }
        const auto* content = S3.GetData().FindPtr("/" + path);
        UNIT_ASSERT_C(content, path);
        return *content;
    }

    void Write(const TString& path, const TString& content) {
        if (IsFs) {
            TFileOutput output(Dir.Name() + "/" + path);
            output.Write(content);
            output.Finish();
        } else {
            S3.GetData()["/" + path] = content;
        }
    }

private:
    const bool IsFs;
    TTempDir Dir;
    TPortManager Ports;
    const ui16 Port;
    TS3Mock S3;
};

enum class EInvalidIndexBackup {
    IncompatibleColumnType,
    TruncatedColumns,
    TruncatedKey,
    MissingImplTable,
};

void MustRebuildIncompatibleIndex(bool isFs, EInvalidIndexBackup problem) {
    TBackupStorage storage(isFs);
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableIndexMaterialization(true).EnableCompactFulltextIndex(false));
    runtime.GetAppData().FeatureFlags.SetEnableFsBackups(isFs);
    // Exercise schema validation on main without depending on encrypted export fixes.
    // Checksums are disabled so that changed schemas reach the compatibility check.
    runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(false);
    runtime.GetAppData().FeatureFlags.SetEnableExportFiltering(false);
    runtime.GetAppData().FeatureFlags.SetEnableChecksumsExport(false);
    runtime.GetAppData().FeatureFlags.SetEnablePermissionsExport(false);
    ui64 txId = 100;

    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table"
        Columns { Name: "key" Type: "Uint64" }
        Columns { Name: "value" Type: "Utf8" }
        Columns { Name: "payload" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);
    for (ui64 row = 1; row <= 3; ++row) {
        const TString value = row < 3 ? "shared words" : "other words";
        const TString payload = ToString(row);
        UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2, 3}, {TCell::Make(row)}, {
            TCell(value.data(), value.size()), TCell(payload.data(), payload.size()),
        });
    }

    Ydb::Table::TableIndex index;
    index.set_name("index");
    index.add_index_columns("value");
    const bool fulltext = problem == EInvalidIndexBackup::MissingImplTable;
    if (fulltext) {
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            columns {
                column: "value"
                analyzers { tokenizer: STANDARD use_filter_lowercase: true }
            }
        )", index.mutable_global_fulltext_relevance_index()->mutable_fulltext_settings()));
    } else {
        index.mutable_global_index();
        index.add_data_columns("payload");
    }
    TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", index);
    env.TestWaitNotification(runtime, txId);
    const auto build = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
    UNIT_ASSERT_VALUES_EQUAL(build.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

    TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
        %s {
            %s
            include_index_data: true
            items { source_path: "/MyRoot/Table" %s: "backup" }
        }
    )", isFs ? "ExportToFsSettings" : "ExportToS3Settings", storage.Settings().c_str(),
        isFs ? "destination_path" : "destination_prefix"));
    env.TestWaitNotification(runtime, txId);
    TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);

    const auto metadata = NBackup::TMetadata::Deserialize(storage.Read("backup/metadata.json"));
    UNIT_ASSERT(metadata.GetIndexes());
    UNIT_ASSERT_VALUES_EQUAL(metadata.GetVersion(), 0);
    if (fulltext) {
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 4);
        NBackup::TMetadata incomplete;
        incomplete.SetVersion(0);
        incomplete.SetEnablePermissions(false);
        for (size_t i = 0; i + 1 < metadata.GetIndexes()->size(); ++i) {
            incomplete.AddIndex(metadata.GetIndexes()->at(i));
        }
        // Keep every file intact: only the metadata's table list is
        // incomplete. The importer must compare it with the full expected set.
        storage.Write("backup/metadata.json", incomplete.Serialize());
    } else {
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
        const TString schemePath = "backup/" + metadata.GetIndexes()->front().ExportPrefix + "/scheme.pb";
        Ydb::Table::CreateTableRequest scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(storage.Read(schemePath), &scheme));
        UNIT_ASSERT_VALUES_EQUAL(scheme.columns_size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(scheme.primary_key_size(), 2);
        switch (problem) {
        case EInvalidIndexBackup::IncompatibleColumnType:
            UNIT_ASSERT_VALUES_EQUAL(scheme.columns(0).name(), "value");
            scheme.mutable_columns(0)->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            break;
        case EInvalidIndexBackup::TruncatedColumns:
            UNIT_ASSERT_VALUES_EQUAL(scheme.columns(2).name(), "payload");
            scheme.mutable_columns()->RemoveLast();
            break;
        case EInvalidIndexBackup::TruncatedKey:
            scheme.mutable_primary_key()->RemoveLast();
            break;
        case EInvalidIndexBackup::MissingImplTable:
            Y_ABORT("Handled above");
        }
        TString content;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(scheme, &content));
        storage.Write(schemePath, content);
    }

    const auto mode = problem == EInvalidIndexBackup::IncompatibleColumnType
        ? Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO
        : Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT;
    for (const bool directImport : {false, true}) {
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(directImport);
        const TString destination = directImport ? "/MyRoot/RestoredDirect" : "/MyRoot/Restored";
        ui32 indexBuilds = 0;
        auto observer = runtime.AddObserver<TEvIndexBuilder::TEvCreateRequest>(
            [&](TEvIndexBuilder::TEvCreateRequest::TPtr&) { ++indexBuilds; });
        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            %s {
                %s
                index_population_mode: %s
                items { %s: "backup" destination_path: "%s" }
            }
        )", isFs ? "ImportFromFsSettings" : "ImportFromS3Settings", storage.Settings().c_str(),
            Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(mode).c_str(),
            isFs ? "source_path" : "source_prefix", destination.c_str()));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL_C(indexBuilds, 1, "An incompatible or incomplete materialized index must be rebuilt");
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard, destination),
            ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table"));
        for (const auto& implTable : *metadata.GetIndexes()) {
            const TString suffix = "/" + implTable.ImplTablePrefix;
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard, destination + suffix),
                ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table" + suffix));
        }
    }
}

void ExportGlobalIndexWithLocalBloom(bool isFs) {
    TBackupStorage storage(isFs);
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableIndexMaterialization(true));
    runtime.GetAppData().FeatureFlags.SetEnableFsBackups(isFs);
    runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);
    runtime.GetAppData().FeatureFlags.SetEnableLocalIndexAsSchemeObject(true);
    ui64 txId = 100;

    TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
        TableDescription {
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "subkey" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key", "subkey"]
            PartitionConfig {
                ByKeyFilterPrefixes { PrefixLength: 1 FalsePositiveProbability: 0.01 }
            }
        }
        IndexDescription {
            Name: "a_bloom"
            Type: EIndexTypeLocalBloomFilter
            State: EIndexStateReady
            KeyColumnNames: ["key"]
            BloomFilterDescription { FalsePositiveProbability: 0.01 }
        }
        IndexDescription {
            Name: "z_global"
            Type: EIndexTypeGlobal
            KeyColumnNames: ["value"]
        }
    )");
    env.TestWaitNotification(runtime, txId);
    const auto description = DescribePath(runtime, "/MyRoot/Table");
    const auto& indexes = description.GetPathDescription().GetTable().GetTableIndexes();
    UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 2);
    // GetChildren() iterates by name. Force the local index to be visited before
    // the global one in ComputeIndexItemSuffix, before filling its encryption IV.
    UNIT_ASSERT_VALUES_EQUAL(indexes[0].GetType(), NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
    UNIT_ASSERT_VALUES_EQUAL(indexes[1].GetType(), NKikimrSchemeOp::EIndexTypeGlobal);

    const TString key = "Very very secret export key!!!!!";
    TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
        %s {
            %s
            %s
            include_index_data: true
            encryption_settings {
                encryption_algorithm: "ChaCha20-Poly1305"
                symmetric_key { key: "%s" }
            }
            items { source_path: "/MyRoot/Table" %s: "backup" }
        }
    )", isFs ? "ExportToFsSettings" : "ExportToS3Settings", storage.Settings().c_str(),
        isFs ? "" : "destination_prefix: \"export\"", key.c_str(),
        isFs ? "destination_path" : "destination_prefix"));
    env.TestWaitNotification(runtime, txId);
    TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);

    const TString prefix = isFs ? "backup/" : "export/backup/";
    const auto decrypt = [&](const TString& path) {
        const TString content = storage.Read(path);
        const auto [plain, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
            NBackup::TEncryptionKey(key), TBuffer(content.data(), content.size()));
        return TString(plain.Data(), plain.Size());
    };
    const auto metadata = NBackup::TMetadata::Deserialize(decrypt(prefix + "metadata.json.enc"));
    UNIT_ASSERT(metadata.GetIndexes());
    UNIT_ASSERT_VALUES_EQUAL(metadata.GetIndexes()->size(), 1);
    const auto& index = metadata.GetIndexes()->front();
    UNIT_ASSERT_VALUES_EQUAL(index.ImplTablePrefix, "z_global/indexImplTable");
    UNIT_ASSERT_VALUES_EQUAL(index.ExportPrefix, "001");
    Ydb::Table::CreateTableRequest scheme;
    UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(
        decrypt(prefix + index.ExportPrefix + "/scheme.pb.enc"), &scheme));
}

} // namespace

Y_UNIT_TEST_SUITE(TMaterializedIndexValidationTests) {
    Y_UNIT_TEST_FLAG(AutoMustRebuildIncompatibleIndex, IsFs) {
        MustRebuildIncompatibleIndex(IsFs, EInvalidIndexBackup::IncompatibleColumnType);
    }

    Y_UNIT_TEST_FLAG(TruncatedColumnsMustRebuildIndex, IsFs) {
        MustRebuildIncompatibleIndex(IsFs, EInvalidIndexBackup::TruncatedColumns);
    }

    Y_UNIT_TEST_FLAG(TruncatedKeyMustRebuildIndex, IsFs) {
        MustRebuildIncompatibleIndex(IsFs, EInvalidIndexBackup::TruncatedKey);
    }

    Y_UNIT_TEST_FLAG(MissingImplTableMustRebuildIndex, IsFs) {
        MustRebuildIncompatibleIndex(IsFs, EInvalidIndexBackup::MissingImplTable);
    }

    Y_UNIT_TEST_FLAG(EncryptedExportWithLocalBloomIndex, IsFs) {
        ExportGlobalIndexWithLocalBloom(IsFs);
    }
}
