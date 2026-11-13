#include "fs_backup_test_base.h"
#include "s3_backup_test_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/stream/file.h>
#include <util/string/subst.h>

using namespace NYdb;

namespace {

template <class TBase>
class TTableChecksumFixture : public TBase {
public:
    TString Prefix = "table";
    static constexpr bool IsFs = std::is_same_v<TBase, TFsBackupTestFixture>;

    void SetUp(NUnitTest::TTestContext&) override {
        auto& flags = this->Server().GetRuntime()->GetAppData().FeatureFlags;
        flags.SetEnableFsBackups(true);
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableChecksumsExport(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        Query(R"(
            CREATE TABLE `/Root/Source` (
                key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)
            );
        )", false);
        Query(R"(UPSERT INTO `/Root/Source` (key, value) VALUES (1ul, "one"), (2ul, NULL), (3ul, "three");)");
    }

    void Query(const TString& sql, bool transactional = true) {
        const auto result = this->YdbQueryClient()
                                .ExecuteQuery(sql, transactional ? NQuery::TTxControl::BeginTx().CommitTx()
                                                                 : NQuery::TTxControl::NoTx())
                                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void Enable(bool value) {
        this->Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableBackupTableChecksum(value);
    }

    void Export(bool encrypted = false, bool compressed = false) {
        if constexpr (IsFs) {
            auto settings = this->MakeExportSettings("");
            settings.AppendItem({ .Src = "/Root/Source", .Dst = Prefix });
            if (encrypted) {
                settings.SymmetricEncryption(
                    NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            }
            if (compressed) {
                settings.Compression("zstd");
            }
            this->WaitOpSuccess(this->YdbExportClient().ExportToFs(settings).GetValueSync());
        } else {
            auto settings = this->MakeExportSettings("", encrypted ? "backup" : "");
            settings.AppendItem({ .Src = "/Root/Source", .Dst = Prefix });
            if (encrypted) {
                settings.SymmetricEncryption(
                    NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            }
            if (compressed) {
                settings.Compression("zstd");
            }
            this->WaitOpSuccess(this->YdbExportClient().ExportToS3(settings).GetValueSync());
        }
    }

    void Import(EStatus expected = EStatus::SUCCESS, bool encrypted = false, const TString& issue = "",
        bool skipChecksumValidation = false) {
        auto check = [&](const auto& result) {
            const auto operation = this->WaitOpStatus(result, expected);
            if (issue) {
                UNIT_ASSERT(operation);
                UNIT_ASSERT_STRING_CONTAINS(operation->Status().GetIssues().ToString(), issue);
            }
        };
        if constexpr (IsFs) {
            auto settings = this->MakeImportSettings("");
            settings.SkipChecksumValidation(skipChecksumValidation);
            if (encrypted) {
                settings.AppendItem({ .Src = "", .Dst = "/Root/Restored", .SrcPathDb = "Source" });
            } else {
                settings.AppendItem({ .Src = Prefix, .Dst = "/Root/Restored" });
            }
            if (encrypted) {
                settings.SymmetricKey("Cool random key!");
            }
            check(this->YdbImportClient().ImportFromFs(settings).GetValueSync());
        } else {
            auto settings = this->MakeImportSettings(encrypted ? "backup" : "", "");
            settings.SkipChecksumValidation(skipChecksumValidation);
            if (encrypted) {
                settings.AppendItem({ .Src = "", .Dst = "/Root/Restored", .SrcPath = "Source" });
            } else {
                settings.AppendItem({ .Src = Prefix, .Dst = "/Root/Restored" });
            }
            if (encrypted) {
                settings.SymmetricKey("Cool random key!");
            }
            check(this->YdbImportClient().ImportFromS3(settings).GetValueSync());
        }
    }

    TString Read(const TString& name, bool encrypted = false) {
        const TString prefix = encrypted && !IsFs ? TString("backup/") + Prefix : Prefix;
        if constexpr (IsFs) {
            return TFileInput(TStringBuilder() << this->GetTempDir().Path() << "/" << prefix << "/" << name).ReadAll();
        } else {
            // The S3 SDK normalizes slashes in object paths
            const TString key = NKikimr::CanonizePath(TStringBuilder() << "/test_bucket/" << prefix << "/" << name);
            const auto* data = this->S3Mock().GetData().FindPtr(key);
            UNIT_ASSERT_C(data, key);
            return *data;
        }
    }

    void Write(const TString& name, const TString& content, bool encrypted = false) {
        const TString prefix = encrypted && !IsFs ? TString("backup/") + Prefix : Prefix;
        if constexpr (IsFs) {
            TFileOutput out(TStringBuilder() << this->GetTempDir().Path() << "/" << prefix << "/" << name);
            out.Write(content);
            out.Finish();
        } else {
            this->S3Mock()
                .GetData()[NKikimr::CanonizePath(TStringBuilder() << "/test_bucket/" << prefix << "/" << name)] =
                content;
        }
    }

    void ReplaceFile(const TString& name, const TString& content) {
        Write(name, content);
        Write(NKikimr::NBackup::ChecksumKey(name), NKikimr::NBackup::ComputeChecksum(content) + " " + name);
    }

    void RunRoundTrip(bool encrypted = false, bool compressed = false) {
        Export(encrypted, compressed);
        TString metadataContent = Read(encrypted ? "metadata.json.enc" : "metadata.json", encrypted);
        if (encrypted) {
            const auto decrypted = NKikimr::NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                NKikimr::NBackup::TEncryptionKey("Cool random key!"),
                TBuffer(metadataContent.data(), metadataContent.size()));
            metadataContent.assign(decrypted.first.Data(), decrypted.first.Size());
        }
        const auto metadata = NKikimr::NBackup::TMetadata::Deserialize(metadataContent, true);
        UNIT_ASSERT(metadata.GetTableChecksum().has_value());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 3);
        Import(EStatus::SUCCESS, encrypted);
        const auto result = this->YdbQueryClient()
                                .ExecuteQuery(R"(
            SELECT count(*) FROM `/Root/Restored`;
        )", NQuery::TTxControl::BeginTx().CommitTx())
                                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 3);
    }

    void RunMismatch(bool enabled, bool encrypted = false, bool skipChecksumValidation = false) {
        Export(encrypted);
        auto data = Read(encrypted ? "data_00.csv.enc" : "data_00.csv", encrypted);
        TMaybe<NKikimr::NBackup::TEncryptionIV> iv;
        if (encrypted) {
            auto decrypted = NKikimr::NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                NKikimr::NBackup::TEncryptionKey("Cool random key!"), TBuffer(data.data(), data.size()));
            data.assign(decrypted.first.Data(), decrypted.first.Size());
            iv = std::move(decrypted.second);
        }
        UNIT_ASSERT(data.Contains("one"));
        SubstGlobal(data, "one", "changed");
        if (encrypted) {
            // Deliberately corrupt the encrypted fixture while keeping file-level checks valid
            const auto changed = NKikimr::NBackup::TEncryptedFileSerializer::EncryptFullFile(
                "AES-128-GCM", NKikimr::NBackup::TEncryptionKey("Cool random key!"), std::move(*iv), data);
            Write("data_00.csv.enc", TString(changed.Data(), changed.Size()), true);
            Write(NKikimr::NBackup::ChecksumKey("data_00.csv"),
                NKikimr::NBackup::ComputeChecksum(data) + " data_00.csv", true);
        } else {
            ReplaceFile("data_00.csv", data);
        }
        Enable(enabled);
        const bool validate = enabled && !skipChecksumValidation;
        Import(validate ? EStatus::CANCELLED : EStatus::SUCCESS, encrypted, validate ? "table checksum mismatch" : "",
            skipChecksumValidation);
        if (skipChecksumValidation) {
            UNIT_ASSERT(Calculate("/Root/Restored") != Calculate("/Root/Source"));
        }
    }

    void MetadataVersion(bool enabled, bool skipChecksumValidation = false) {
        Export();
        NJson::TJsonValue metadata;
        UNIT_ASSERT(NJson::ReadJsonTree(Read("metadata.json"), &metadata));
        metadata["table_checksum"]["version"] = 999;
        ReplaceFile("metadata.json", NJson::WriteJson(&metadata, false));
        Enable(enabled);
        Import(EStatus::SUCCESS, false, "", skipChecksumValidation);
        UNIT_ASSERT(Calculate("/Root/Restored") == Calculate("/Root/Source"));
    }

    void RunMalformedChecksum(bool skipChecksumValidation) {
        Export();
        NJson::TJsonValue metadata;
        UNIT_ASSERT(NJson::ReadJsonTree(Read("metadata.json"), &metadata));
        metadata["table_checksum"]["count"] = "invalid";
        ReplaceFile("metadata.json", NJson::WriteJson(&metadata, false));
        Import(skipChecksumValidation ? EStatus::SUCCESS : EStatus::CANCELLED, false,
            skipChecksumValidation ? "" : "invalid table_checksum.count", skipChecksumValidation);
        if (skipChecksumValidation) {
            UNIT_ASSERT(Calculate("/Root/Restored") == Calculate("/Root/Source"));
        }
    }

    void RunLegacy() {
        Enable(false);
        Export();
        UNIT_ASSERT(!NKikimr::NBackup::TMetadata::Deserialize(Read("metadata.json"), true).GetTableChecksum());
        Enable(true);
        Import();
    }

    NKikimr::NBackup::TTableChecksum Calculate(const TString& path) {
        const auto result =
            this->YdbQueryClient()
                .ExecuteQuery(TStringBuilder()
                                  << "SELECT count(*), SUM(Digest::CityHash(StablePickle(TableRow()))) FROM `" << path
                                  << "`;", NQuery::TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        return { .Count = parser.ColumnParser(0).GetUint64(),
            .Hash = parser.ColumnParser(1).GetOptionalUint64().value_or(0) };
    }

    void RunPartitionIndependence() {
        Query(R"(
            CREATE TABLE `/Root/Other` (
                key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)
            ) WITH (UNIFORM_PARTITIONS = 4);
        )", false);
        Query(R"(UPSERT INTO `/Root/Other` (key, value) VALUES (3ul, "three"), (1ul, "one"), (2ul, NULL);)");
        UNIT_ASSERT(Calculate("/Root/Source") == Calculate("/Root/Other"));
        const auto original = Calculate("/Root/Other");
        Query(R"(UPSERT INTO `/Root/Other` (key, value) VALUES (2ul, "");)");
        UNIT_ASSERT(original != Calculate("/Root/Other"));
    }

    void RunChangefeeds(bool enabled) {
        auto& runtime = *this->Server().GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableChangefeedsExport(enabled);
            runtime.GetAppData(node).FeatureFlags.SetEnableChangefeedsImport(true);
        }
        Query(R"(
            ALTER TABLE `/Root/Source` ADD CHANGEFEED `stream` WITH (
                FORMAT = 'JSON', MODE = 'UPDATES'
            );
        )", false);
        Export();
        const auto metadata = NKikimr::NBackup::TMetadata::Deserialize(Read("metadata.json"), true);
        UNIT_ASSERT(metadata.GetTableChecksum());
        UNIT_ASSERT(metadata.GetChangefeeds());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetChangefeeds()->size(), enabled ? 1 : 0);
        if (enabled) {
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetChangefeeds()->front().Name, "stream");
            UNIT_ASSERT(!Read("stream/changefeed_description.pb").empty());
        }
        Import();
    }

    void RunEmpty() {
        Query("DELETE FROM `/Root/Source`;");
        Export();
        const auto metadata = NKikimr::NBackup::TMetadata::Deserialize(Read("metadata.json"), true);
        UNIT_ASSERT(metadata.GetTableChecksum());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Hash, 0);
        Import();
    }
};

} // namespace

Y_UNIT_TEST_SUITE_F(TableChecksumsS3, TTableChecksumFixture<TS3BackupTestFixture>) {
    Y_UNIT_TEST(RoundTrip) {
        RunRoundTrip();
    }
    Y_UNIT_TEST(TrailingSlashPrefix) {
        Prefix = "table/";
        RunRoundTrip();
    }
    Y_UNIT_TEST(Empty) {
        RunEmpty();
    }
    Y_UNIT_TEST(PartitionIndependence) {
        RunPartitionIndependence();
    }
    Y_UNIT_TEST(Encrypted) {
        RunRoundTrip(true);
    }
    Y_UNIT_TEST(EncryptedMismatch) {
        RunMismatch(true, true);
    }
    Y_UNIT_TEST(Compressed) {
        RunRoundTrip(false, true);
    }
    Y_UNIT_TEST(Mismatch) {
        RunMismatch(true);
    }
    Y_UNIT_TEST(DisabledIgnoresMismatch) {
        RunMismatch(false);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresMismatch) {
        RunMismatch(true, false, true);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresUnknownVersion) {
        MetadataVersion(true, true);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresMalformedChecksum) {
        RunMalformedChecksum(true);
    }
    Y_UNIT_TEST(MalformedChecksum) {
        RunMalformedChecksum(false);
    }
    Y_UNIT_TEST(UnknownVersion) {
        MetadataVersion(true);
    }
    Y_UNIT_TEST(DisabledIgnoresVersion) {
        MetadataVersion(false);
    }
    Y_UNIT_TEST(Legacy) {
        RunLegacy();
    }
    Y_UNIT_TEST(ChangefeedsEnabled) {
        RunChangefeeds(true);
    }
    Y_UNIT_TEST(ChangefeedsDisabled) {
        RunChangefeeds(false);
    }
}

Y_UNIT_TEST_SUITE_F(TableChecksumsFs, TTableChecksumFixture<TFsBackupTestFixture>) {
    Y_UNIT_TEST(RoundTrip) {
        RunRoundTrip();
    }
    Y_UNIT_TEST(TrailingSlashPrefix) {
        Prefix = "table/";
        RunRoundTrip();
    }
    Y_UNIT_TEST(Empty) {
        RunEmpty();
    }
    Y_UNIT_TEST(PartitionIndependence) {
        RunPartitionIndependence();
    }
    Y_UNIT_TEST(Encrypted) {
        RunRoundTrip(true);
    }
    Y_UNIT_TEST(EncryptedMismatch) {
        RunMismatch(true, true);
    }
    Y_UNIT_TEST(Compressed) {
        RunRoundTrip(false, true);
    }
    Y_UNIT_TEST(Mismatch) {
        RunMismatch(true);
    }
    Y_UNIT_TEST(DisabledIgnoresMismatch) {
        RunMismatch(false);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresMismatch) {
        RunMismatch(true, false, true);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresUnknownVersion) {
        MetadataVersion(true, true);
    }
    Y_UNIT_TEST(SkipChecksumValidationIgnoresMalformedChecksum) {
        RunMalformedChecksum(true);
    }
    Y_UNIT_TEST(MalformedChecksum) {
        RunMalformedChecksum(false);
    }
    Y_UNIT_TEST(UnknownVersion) {
        MetadataVersion(true);
    }
    Y_UNIT_TEST(DisabledIgnoresVersion) {
        MetadataVersion(false);
    }
    Y_UNIT_TEST(Legacy) {
        RunLegacy();
    }
    Y_UNIT_TEST(ChangefeedsEnabled) {
        RunChangefeeds(true);
    }
    Y_UNIT_TEST(ChangefeedsDisabled) {
        RunChangefeeds(false);
    }
}
