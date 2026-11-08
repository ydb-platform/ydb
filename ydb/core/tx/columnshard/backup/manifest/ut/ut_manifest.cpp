#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/tx/columnshard/backup/manifest/manifest.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NBackup {

Y_UNIT_TEST_SUITE(DataManifest) {
    Y_UNIT_TEST(Roundtrip) {
        TDataManifest manifest;
        manifest.FilesCount = 12345;
        auto parsed = TDataManifest::Parse(manifest.Serialize());
        UNIT_ASSERT_C(parsed.IsSuccess(), parsed.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(parsed->FilesCount, 12345);
        UNIT_ASSERT_VALUES_EQUAL(parsed->Version, TDataManifest::CurrentVersion);
    }

    Y_UNIT_TEST(Invalid) {
        UNIT_ASSERT(TDataManifest::Parse("not a json").IsFail());
        UNIT_ASSERT(TDataManifest::Parse(R"({"files_count":1})").IsFail());
        UNIT_ASSERT(TDataManifest::Parse(R"({"version":1,"files_count":0})").IsFail());
        UNIT_ASSERT(TDataManifest::Parse(R"({"version":100,"files_count":1})").IsFail());
    }

    Y_UNIT_TEST(DataFileNames) {
        UNIT_ASSERT_VALUES_EQUAL(DataFileObjectKeyPattern("backup/table", 3, 0), "backup/table");
        UNIT_ASSERT_VALUES_EQUAL(DataFileObjectKeyPattern("backup/table", 3, 42), "backup/table/data_03_files/000042");
        UNIT_ASSERT_VALUES_EQUAL(DataManifestKeySuffix(3), "data_03.manifest.json");

        NKikimrSchemeOp::TRestoreTask task;
        task.SetShardNum(1);
        task.MutableS3Settings()->SetObjectKeyPattern("backup/table");
        SetDataFile(task, 0);
        UNIT_ASSERT_VALUES_EQUAL(task.GetS3Settings().GetObjectKeyPattern(), "backup/table");
        SetDataFile(task, 7);
        UNIT_ASSERT_VALUES_EQUAL(task.GetS3Settings().GetObjectKeyPattern(), "backup/table/data_01_files/000007");
        UNIT_ASSERT(!task.HasEncryptionSettings());
    }

    Y_UNIT_TEST(DataFileIVsAreUnique) {
        using NKikimr::NBackup::EBackupFileType;
        using NKikimr::NBackup::TEncryptionIV;
        const auto base = TEncryptionIV::Generate();
        THashSet<TString> ivs;
        // The other files of the backup item
        for (auto type :
            { EBackupFileType::TableSchema, EBackupFileType::Metadata, EBackupFileType::Permissions, EBackupFileType::TableChangefeed }) {
            ivs.insert(TEncryptionIV::Combine(base, type, 0, 0).GetBinaryString());
        }
        for (ui32 file = 0; file < 64; ++file) {
            const auto fileBase = TEncryptionIV::FromBinaryString(DataFileIV(base.GetBinaryString(), file));
            for (ui32 shard = 0; shard < 8; ++shard) {
                ivs.insert(TEncryptionIV::Combine(fileBase, EBackupFileType::TableData, 0, shard).GetBinaryString());
            }
            // The item files written by the uploaders of the data files of shard 0
            if (file) {
                for (auto type : { EBackupFileType::TableSchema, EBackupFileType::Metadata, EBackupFileType::Permissions,
                         EBackupFileType::TableChangefeed }) {
                    ivs.insert(TEncryptionIV::Combine(fileBase, type, 0, 0).GetBinaryString());
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ivs.size(), 4 + 64 * 8 + 63 * 4);
        UNIT_ASSERT_VALUES_EQUAL(DataFileIV(base.GetBinaryString(), 0), base.GetBinaryString());
        UNIT_ASSERT_EXCEPTION(DataFileIV(base.GetBinaryString(), MaxDataFileIndex), yexception);
    }
}

}   // namespace NKikimr::NColumnShard::NBackup
