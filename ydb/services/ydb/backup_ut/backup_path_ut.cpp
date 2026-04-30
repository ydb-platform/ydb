#include "s3_backup_test_base.h"
#include "fs_backup_test_base.h"

#include <util/random/random.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>

#include <ydb/library/testlib/helpers.h>

using namespace NYdb;

using TBackupPathTestFixture = TS3BackupTestFixture;
using TBackupPathTestFixtureFs = TFsBackupTestFixture;

namespace {

template <typename TExportSettings>
struct TBackupTraits;

template <>
struct TBackupTraits<NExport::TExportToS3Settings> {
    using TExportSettings = NExport::TExportToS3Settings;
    using TImportSettings = NImport::TImportFromS3Settings;
    using TExportResponse = NExport::TExportToS3Response;

    TExportSettings MakeExportSettings(TS3BackupTestFixture& f, const TString& sourcePath) {
        return f.MakeExportSettings(sourcePath, "Prefix");
    }

    TImportSettings MakeImportSettings(TS3BackupTestFixture& f, const TString& dstPath) {
        return f.MakeImportSettings("Prefix", dstPath);
    }

    auto Export(TS3BackupTestFixture& f, const TExportSettings& settings) {
        return f.YdbExportClient().ExportToS3(settings).GetValueSync();
    }

    auto Import(TS3BackupTestFixture& f, const TImportSettings& settings) {
        return f.YdbImportClient().ImportFromS3(settings).GetValueSync();
    }

    void ValidateFileList(TS3BackupTestFixture& f, const TSet<TString>& paths) {
        f.ValidateS3FileList(paths);
    }

    TString FilePrefix() {
        return "/test_bucket/Prefix/";
    }

    TString FilePrefixRaw() {
        return "/test_bucket/";
    }

    static TImportSettings::TItem MakeImportItem(const TString& dst, const TString& srcPath) {
        return TImportSettings::TItem{.Dst = dst, .SrcPath = srcPath};
    }

    static TImportSettings::TItem MakeImportItemSrcPathOnly(const TString& srcPath) {
        return TImportSettings::TItem{.SrcPath = srcPath};
    }

    static TImportSettings::TItem MakeImportItemWithSrcDstAndSrcPath(const TString& src, const TString& dst, const TString& srcPath) {
        return TImportSettings::TItem{.Src = src, .Dst = dst, .SrcPath = srcPath};
    }

    TExportSettings MakeExportSettingsNoPrefix(TS3BackupTestFixture& f, const TString& sourcePath) {
        return f.MakeExportSettings(sourcePath, "");
    }

    TExportSettings MakeExportSettingsRaw(TS3BackupTestFixture& f) {
        return f.MakeExportSettings("", "");
    }

    TImportSettings MakeImportSettingsRaw(TS3BackupTestFixture& f) {
        return f.MakeImportSettings("", "");
    }

    TImportSettings MakeImportSettingsNoDestination(TS3BackupTestFixture& f) {
        return f.MakeImportSettings("Prefix", "");
    }

    TString ImportSrcPrefix() {
        return "Prefix/";
    }

    static void SetEncryption(TExportSettings& settings, const std::string& algorithm, const TString& key) {
        settings.SymmetricEncryption(algorithm.c_str(), key.c_str());
    }

    static void SetDecryptionKey(TImportSettings& settings, const TString& key) {
        settings.SymmetricKey(key.c_str());
    }

    static TString EncryptedFileExtension() {
        return ".enc";
    }

    auto ExportAsync(TS3BackupTestFixture& f, const TExportSettings& settings) {
        return f.YdbExportClient().ExportToS3(settings);
    }

    TExportSettings MakeExportSettingsCustomDest(TS3BackupTestFixture& f, const TString& sourcePath, const TString& dest) {
        return f.MakeExportSettings(sourcePath, dest);
    }

    TImportSettings MakeImportSettingsCustomSource(TS3BackupTestFixture& f, const TString& source, const TString& dstPath) {
        return f.MakeImportSettings(source, dstPath);
    }

    static void SetCompression(TExportSettings& settings, const std::string& compression) {
        settings.Compression(compression.c_str());
    }

    void ModifyChecksumAndCheckThatImportFails(TS3BackupTestFixture& f, const std::initializer_list<TString>& checksumFiles, const TImportSettings& importSettings) {
        f.ModifyChecksumAndCheckThatImportFails(checksumFiles, importSettings);
    }
};

template <>
struct TBackupTraits<NExport::TExportToFsSettings> {
    using TExportSettings = NExport::TExportToFsSettings;
    using TImportSettings = NImport::TImportFromFsSettings;
    using TExportResponse = NExport::TExportToFsResponse;

    TExportSettings MakeExportSettings(TFsBackupTestFixture& f, const TString& sourcePath) {
        return f.MakeExportSettings(sourcePath);
    }

    TImportSettings MakeImportSettings(TFsBackupTestFixture& f, const TString& dstPath) {
        return f.MakeImportSettings(dstPath);
    }

    auto Export(TFsBackupTestFixture& f, const TExportSettings& settings) {
        return f.YdbExportClient().ExportToFs(settings).GetValueSync();
    }

    auto Import(TFsBackupTestFixture& f, const TImportSettings& settings) {
        return f.YdbImportClient().ImportFromFs(settings).GetValueSync();
    }

    void ValidateFileList(TFsBackupTestFixture& f, const TSet<TString>& paths) {
        f.ValidateFileList(paths);
    }

    TString FilePrefix() {
        return "";
    }

    TString FilePrefixRaw() {
        return "";
    }

    static TImportSettings::TItem MakeImportItem(const TString& dst, const TString& srcPath) {
        return TImportSettings::TItem{.Dst = dst, .SrcPathDb = srcPath};
    }

    static TImportSettings::TItem MakeImportItemSrcPathOnly(const TString& srcPath) {
        return TImportSettings::TItem{.SrcPathDb = srcPath};
    }

    static TImportSettings::TItem MakeImportItemWithSrcDstAndSrcPath(const TString& src, const TString& dst, const TString& srcPath) {
        return TImportSettings::TItem{.Src = src, .Dst = dst, .SrcPathDb = srcPath};
    }

    TExportSettings MakeExportSettingsNoPrefix(TFsBackupTestFixture& f, const TString& sourcePath) {
        return f.MakeExportSettings(sourcePath);
    }

    TExportSettings MakeExportSettingsRaw(TFsBackupTestFixture& f) {
        return f.MakeExportSettings("");
    }

    TImportSettings MakeImportSettingsRaw(TFsBackupTestFixture& f) {
        return f.MakeImportSettings("");
    }

    TImportSettings MakeImportSettingsNoDestination(TFsBackupTestFixture& f) {
        return f.MakeImportSettings("");
    }

    TString ImportSrcPrefix() {
        return "";
    }

    static void SetEncryption(TExportSettings& settings, const std::string& algorithm, const TString& key) {
        settings.SymmetricEncryption(algorithm.c_str(), key.c_str());
    }

    static void SetDecryptionKey(TImportSettings& settings, const TString& key) {
        settings.SymmetricKey(key.c_str());
    }

    static TString EncryptedFileExtension() {
        return ".enc";
    }

    auto ExportAsync(TFsBackupTestFixture& f, const TExportSettings& settings) {
        return f.YdbExportClient().ExportToFs(settings);
    }

    TExportSettings MakeExportSettingsCustomDest(TFsBackupTestFixture& f, const TString& sourcePath, const TString& dest) {
        TExportSettings settings;
        if (dest) {
            TString basePath = TString(f.GetTempDir().Path()) + "/" + dest;
            TFsPath(basePath).MkDirs();
            settings.BasePath(basePath);
        } else {
            settings.BasePath(TString(f.GetTempDir().Path()));
        }
        if (sourcePath) {
            settings.SourcePath(sourcePath);
        }
        return settings;
    }

    TImportSettings MakeImportSettingsCustomSource(TFsBackupTestFixture& f, const TString& source, const TString& dstPath) {
        TImportSettings settings;
        if (source) {
            settings.BasePath(TString(f.GetTempDir().Path()) + "/" + source);
        } else {
            settings.BasePath(TString(f.GetTempDir().Path()));
        }
        if (dstPath) {
            settings.DestinationPath(dstPath);
        }
        return settings;
    }

    static void SetCompression(TExportSettings& settings, const std::string& compression) {
        settings.Compression(compression.c_str());
    }

    void ModifyChecksumAndCheckThatImportFails(TFsBackupTestFixture& f, const std::initializer_list<TString>& checksumFiles, const TImportSettings& importSettings) {
        f.ModifyChecksumAndCheckThatImportFails(checksumFiles, importSettings);
    }
};

template <typename TExportSettings, typename TBackupTestFixture>
void ImportFilterByYdbObjectPathImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();

    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        exportSettings
                .AppendItem(typename TExportSettings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
                .AppendItem(typename TExportSettings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
                .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2_Prefix"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table0_Prefix/metadata.json",
            prefix + "Table0_Prefix/scheme.pb",
            prefix + "Table0_Prefix/permissions.pb",
            prefix + "Table0_Prefix/data_00.csv",
            prefix + "Table1_Prefix/metadata.json",
            prefix + "Table1_Prefix/scheme.pb",
            prefix + "Table1_Prefix/permissions.pb",
            prefix + "Table1_Prefix/data_00.csv",
            prefix + "Table2_Prefix/metadata.json",
            prefix + "Table2_Prefix/scheme.pb",
            prefix + "Table2_Prefix/permissions.pb",
            prefix + "Table2_Prefix/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table0_Prefix/metadata.json.sha256",
            prefix + "Table0_Prefix/scheme.pb.sha256",
            prefix + "Table0_Prefix/permissions.pb.sha256",
            prefix + "Table0_Prefix/data_00.csv.sha256",
            prefix + "Table1_Prefix/metadata.json.sha256",
            prefix + "Table1_Prefix/scheme.pb.sha256",
            prefix + "Table1_Prefix/permissions.pb.sha256",
            prefix + "Table1_Prefix/data_00.csv.sha256",
            prefix + "Table2_Prefix/metadata.json.sha256",
            prefix + "Table2_Prefix/scheme.pb.sha256",
            prefix + "Table2_Prefix/permissions.pb.sha256",
            prefix + "Table2_Prefix/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings
                .AppendItem(traits.MakeImportItem("/Root/RestorePrefix/Table123", "dir1/dir2//Table2"))
                .AppendItem(traits.MakeImportItem("/Root/RestorePrefix/Table321", "Table0"));
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table123", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table321", isOlap),
        });
        f.ValidateDoesNotHaveYdbTables({
            "/Root/RestorePrefix/Table0",
            "/Root/RestorePrefix/dir1/Table1",
            "/Root/RestorePrefix/dir1/dir2/Table2",
        });
    }

    // Recursive filter by directory
    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix2");
        importSettings.AppendItem(traits.MakeImportItemSrcPathOnly("dir1"));
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TS3BackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix2/dir1/Table1", isOlap),
            TS3BackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix2/dir1/dir2/Table2", isOlap),
        });
        f.ValidateDoesNotHaveYdbTables({
            "/Root/RestorePrefix2/Table0",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings.AppendItem(traits.MakeImportItemWithSrcDstAndSrcPath("/Root/RestorePrefix/dir1/dir2/Table2", "/Root/RestorePrefix/Table0", "dir1/dir2/Table2"));
        UNIT_ASSERT_EXCEPTION(traits.Import(f, importSettings), TContractViolation);
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExplicitDuplicatedItemsImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1");
    exportSettings
        .AppendItem(typename TExportSettings::TItem{.Src = "dir2"})
        .AppendItem(typename TExportSettings::TItem{.Src = "/dir2"})
        .AppendItem(typename TExportSettings::TItem{.Src = "dir2/"});
    auto res = traits.Export(f, exportSettings);
    f.WaitOpStatus(res, EStatus::BAD_REQUEST);
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportUnexistingExplicitPathImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1");
    exportSettings
        .AppendItem(typename TExportSettings::TItem{.Src = "unexisting"});
    auto res = traits.Export(f, exportSettings);
    f.WaitOpStatus(res, EStatus::SCHEME_ERROR);
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportUnexistingCommonSourcePathImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/unexisting");
    auto res = traits.Export(f, exportSettings);
    f.WaitOpStatus(res, EStatus::SCHEME_ERROR);
}

template <typename TExportSettings, typename TBackupTestFixture>
void OnlyOneEmptyDirectoryImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    auto exportSettings = traits.MakeExportSettings(f, "");
    exportSettings
        .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/dir3"});
    auto res = traits.Export(f, exportSettings);
    UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportWholeDatabaseImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "RecursiveFolderProcessing/Table0/metadata.json",
            prefix + "RecursiveFolderProcessing/Table0/scheme.pb",
            prefix + "RecursiveFolderProcessing/Table0/permissions.pb",
            prefix + "RecursiveFolderProcessing/Table0/data_00.csv",
            prefix + "RecursiveFolderProcessing/dir1/Table1/metadata.json",
            prefix + "RecursiveFolderProcessing/dir1/Table1/scheme.pb",
            prefix + "RecursiveFolderProcessing/dir1/Table1/permissions.pb",
            prefix + "RecursiveFolderProcessing/dir1/Table1/data_00.csv",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "RecursiveFolderProcessing/Table0/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/Table0/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/Table0/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/Table0/data_00.csv.sha256",
            prefix + "RecursiveFolderProcessing/dir1/Table1/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/dir1/Table1/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/Table1/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/Table1/data_00.csv.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportWithCommonSourcePathImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table1/metadata.json",
            prefix + "Table1/scheme.pb",
            prefix + "Table1/permissions.pb",
            prefix + "Table1/data_00.csv",
            prefix + "dir2/Table2/metadata.json",
            prefix + "dir2/Table2/scheme.pb",
            prefix + "dir2/Table2/permissions.pb",
            prefix + "dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table1/metadata.json.sha256",
            prefix + "Table1/scheme.pb.sha256",
            prefix + "Table1/permissions.pb.sha256",
            prefix + "Table1/data_00.csv.sha256",
            prefix + "dir2/Table2/metadata.json.sha256",
            prefix + "dir2/Table2/scheme.pb.sha256",
            prefix + "dir2/Table2/permissions.pb.sha256",
            prefix + "dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportWithExcludeRegexpsImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        exportSettings.AppendExcludeRegexp(".*");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpStatus(res, EStatus::BAD_REQUEST);
    }

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        exportSettings.AppendExcludeRegexp("invalid regexp)");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpStatus(res, EStatus::BAD_REQUEST);
    }

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        exportSettings
            .AppendExcludeRegexp("^Table$")
            .AppendExcludeRegexp("^dir1$")
            .AppendExcludeRegexp("^dir1/Table");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table0/metadata.json",
            prefix + "Table0/scheme.pb",
            prefix + "Table0/permissions.pb",
            prefix + "Table0/data_00.csv",
            prefix + "dir1/dir2/Table2/metadata.json",
            prefix + "dir1/dir2/Table2/scheme.pb",
            prefix + "dir1/dir2/Table2/permissions.pb",
            prefix + "dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table0/metadata.json.sha256",
            prefix + "Table0/scheme.pb.sha256",
            prefix + "Table0/permissions.pb.sha256",
            prefix + "Table0/data_00.csv.sha256",
            prefix + "dir1/dir2/Table2/metadata.json.sha256",
            prefix + "dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportWithCommonSourcePathAndExplicitTableInsideImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/Table1", .Dst = "ExplicitTable1Prefix"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "ExplicitTable1Prefix/metadata.json",
            prefix + "ExplicitTable1Prefix/scheme.pb",
            prefix + "ExplicitTable1Prefix/permissions.pb",
            prefix + "ExplicitTable1Prefix/data_00.csv",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "ExplicitTable1Prefix/metadata.json.sha256",
            prefix + "ExplicitTable1Prefix/scheme.pb.sha256",
            prefix + "ExplicitTable1Prefix/permissions.pb.sha256",
            prefix + "ExplicitTable1Prefix/data_00.csv.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void RecursiveDirectoryPlusExplicitTableImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/Table0"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "RecursiveFolderProcessing/Table0/metadata.json",
            prefix + "RecursiveFolderProcessing/Table0/scheme.pb",
            prefix + "RecursiveFolderProcessing/Table0/permissions.pb",
            prefix + "RecursiveFolderProcessing/Table0/data_00.csv",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "RecursiveFolderProcessing/Table0/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/Table0/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/Table0/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/Table0/data_00.csv.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void EmptyDirectoryIsOkImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1/dir2");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Table2"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/dir3"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table2/metadata.json",
            prefix + "Table2/scheme.pb",
            prefix + "Table2/permissions.pb",
            prefix + "Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table2/metadata.json.sha256",
            prefix + "Table2/scheme.pb.sha256",
            prefix + "Table2/permissions.pb.sha256",
            prefix + "Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ImportWithExcludeRegexpsImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table0/metadata.json",
            prefix + "Table0/scheme.pb",
            prefix + "Table0/permissions.pb",
            prefix + "Table0/data_00.csv",
            prefix + "dir1/Table1/metadata.json",
            prefix + "dir1/Table1/scheme.pb",
            prefix + "dir1/Table1/permissions.pb",
            prefix + "dir1/Table1/data_00.csv",
            prefix + "dir1/dir2/Table2/metadata.json",
            prefix + "dir1/dir2/Table2/scheme.pb",
            prefix + "dir1/dir2/Table2/permissions.pb",
            prefix + "dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table0/metadata.json.sha256",
            prefix + "Table0/scheme.pb.sha256",
            prefix + "Table0/permissions.pb.sha256",
            prefix + "Table0/data_00.csv.sha256",
            prefix + "dir1/Table1/metadata.json.sha256",
            prefix + "dir1/Table1/scheme.pb.sha256",
            prefix + "dir1/Table1/permissions.pb.sha256",
            prefix + "dir1/Table1/data_00.csv.sha256",
            prefix + "dir1/dir2/Table2/metadata.json.sha256",
            prefix + "dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings.AppendExcludeRegexp(".*");
        auto res = traits.Import(f, importSettings);
        f.WaitOpStatus(res, EStatus::CANCELLED);
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings.AppendExcludeRegexp("invalid regexp)");
        auto res = traits.Import(f, importSettings);
        f.WaitOpStatus(res, EStatus::BAD_REQUEST);
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings
            .AppendExcludeRegexp("^Table$")
            .AppendExcludeRegexp("^dir1$")
            .AppendExcludeRegexp("^dir1/Table");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", isOlap),
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix2");
        importSettings
            .AppendItem(traits.MakeImportItemSrcPathOnly("dir1"))
            .AppendExcludeRegexp("Table1");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix2/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ImportFilterByPrefixImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
            .AppendItem(typename TExportSettings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
            .AppendItem(typename TExportSettings::TItem{.Src = "dir1/dir2/Table2", .Dst = "Table2_Prefix"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table0_Prefix/metadata.json",
            prefix + "Table0_Prefix/scheme.pb",
            prefix + "Table0_Prefix/permissions.pb",
            prefix + "Table0_Prefix/data_00.csv",
            prefix + "Table1_Prefix/metadata.json",
            prefix + "Table1_Prefix/scheme.pb",
            prefix + "Table1_Prefix/permissions.pb",
            prefix + "Table1_Prefix/data_00.csv",
            prefix + "Table2_Prefix/metadata.json",
            prefix + "Table2_Prefix/scheme.pb",
            prefix + "Table2_Prefix/permissions.pb",
            prefix + "Table2_Prefix/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table0_Prefix/metadata.json.sha256",
            prefix + "Table0_Prefix/scheme.pb.sha256",
            prefix + "Table0_Prefix/permissions.pb.sha256",
            prefix + "Table0_Prefix/data_00.csv.sha256",
            prefix + "Table1_Prefix/metadata.json.sha256",
            prefix + "Table1_Prefix/scheme.pb.sha256",
            prefix + "Table1_Prefix/permissions.pb.sha256",
            prefix + "Table1_Prefix/data_00.csv.sha256",
            prefix + "Table2_Prefix/metadata.json.sha256",
            prefix + "Table2_Prefix/scheme.pb.sha256",
            prefix + "Table2_Prefix/permissions.pb.sha256",
            prefix + "Table2_Prefix/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        importSettings
            .AppendItem(typename TImportSettings::TItem{.Src = "Table0_Prefix", .Dst = "/Root/RestorePrefix/Table0"});
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table0", isOlap),
        });
        f.ValidateDoesNotHaveYdbTables({
            "/Root/RestorePrefix/dir1/Table1",
            "/Root/RestorePrefix/dir1/dir2/Table2",
        });
    }
}

// This test is S3-specific
template <typename TExportSettings, typename TBackupTestFixture>
void FilterByPathFailsWhenNoSchemaMappingImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    const TString prefixRaw = traits.FilePrefixRaw();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);
    if constexpr (std::is_same_v<TExportSettings, NExport::TExportToFsSettings>) {
        f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableEncryptedExport(false);
    }
    {
        auto exportSettings = traits.MakeExportSettingsNoPrefix(f, "/Root/RecursiveFolderProcessing/dir1");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Table1", .Dst = "Prefix/t1"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefixRaw + "Prefix/t1/metadata.json",
            prefixRaw + "Prefix/t1/scheme.pb",
            prefixRaw + "Prefix/t1/permissions.pb",
            prefixRaw + "Prefix/t1/data_00.csv",

            prefixRaw + "Prefix/t1/metadata.json.sha256",
            prefixRaw + "Prefix/t1/scheme.pb.sha256",
            prefixRaw + "Prefix/t1/permissions.pb.sha256",
            prefixRaw + "Prefix/t1/data_00.csv.sha256",
        });
    }

    {
        // Import with explicit item but using SrcPath (YDB path) - fails because no SchemaMapping
        auto importSettings = traits.MakeImportSettingsRaw(f);
        importSettings
            .AppendItem(traits.MakeImportItem("/Root/RestorePrefix/Table1", "/Root/RestorePrefix/Table1"));
        auto res = traits.Import(f, importSettings);
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
    }

    {
        // Import with source_prefix but no items - tries to load SchemaMapping which doesn't exist
        auto importSettings = traits.MakeImportSettingsNoDestination(f);
        auto res = traits.Import(f, importSettings);
        f.WaitOpStatus(res, EStatus::CANCELLED);
    }

    {
        // Import with source_prefix and item using SrcPath - fails because no SchemaMapping
        auto importSettings = traits.MakeImportSettingsNoDestination(f);
        importSettings
            .AppendItem(traits.MakeImportItem("/Root/RestorePrefix/Table1", "/Root/RestorePrefix/Table1"));
        auto res = traits.Import(f, importSettings);
        f.WaitOpStatus(res, EStatus::CANCELLED);
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void CommonPrefixButExplicitImportItemsImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    const TString prefix = traits.FilePrefix();
    const TString importPrefix = traits.ImportSrcPrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table1/metadata.json",
            prefix + "Table1/scheme.pb",
            prefix + "Table1/permissions.pb",
            prefix + "Table1/data_00.csv",
            prefix + "dir2/Table2/metadata.json",
            prefix + "dir2/Table2/scheme.pb",
            prefix + "dir2/Table2/permissions.pb",
            prefix + "dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table1/metadata.json.sha256",
            prefix + "Table1/scheme.pb.sha256",
            prefix + "Table1/permissions.pb.sha256",
            prefix + "Table1/data_00.csv.sha256",
            prefix + "dir2/Table2/metadata.json.sha256",
            prefix + "dir2/Table2/scheme.pb.sha256",
            prefix + "dir2/Table2/permissions.pb.sha256",
            prefix + "dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettingsRaw(f);
        importSettings
            .AppendItem(typename TImportSettings::TItem{.Src = importPrefix + "Table1", .Dst = "/Root/RestorePrefix/Table1"})
            .AppendItem(typename TImportSettings::TItem{.Src = importPrefix + "dir2/Table2", .Dst = "/Root/RestorePrefix/dir2/yet/another/dir/Table2"});
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir2/yet/another/dir/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportCommonSourcePathImportExplicitlyImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    const TString prefix = traits.FilePrefix();
    const TString importPrefix = traits.ImportSrcPrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/mapping.json",
            prefix + "Table0/metadata.json",
            prefix + "Table0/scheme.pb",
            prefix + "Table0/permissions.pb",
            prefix + "Table0/data_00.csv",
            prefix + "dir1/Table1/metadata.json",
            prefix + "dir1/Table1/scheme.pb",
            prefix + "dir1/Table1/permissions.pb",
            prefix + "dir1/Table1/data_00.csv",
            prefix + "dir1/dir2/Table2/metadata.json",
            prefix + "dir1/dir2/Table2/scheme.pb",
            prefix + "dir1/dir2/Table2/permissions.pb",
            prefix + "dir1/dir2/Table2/data_00.csv",

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table0/metadata.json.sha256",
            prefix + "Table0/scheme.pb.sha256",
            prefix + "Table0/permissions.pb.sha256",
            prefix + "Table0/data_00.csv.sha256",
            prefix + "dir1/Table1/metadata.json.sha256",
            prefix + "dir1/Table1/scheme.pb.sha256",
            prefix + "dir1/Table1/permissions.pb.sha256",
            prefix + "dir1/Table1/data_00.csv.sha256",
            prefix + "dir1/dir2/Table2/metadata.json.sha256",
            prefix + "dir1/dir2/Table2/scheme.pb.sha256",
            prefix + "dir1/dir2/Table2/permissions.pb.sha256",
            prefix + "dir1/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettingsRaw(f);
        importSettings
            .AppendItem(typename TImportSettings::TItem{.Src = importPrefix + "Table0", .Dst = "/Root/RestorePrefix/Table0"});
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table0", isOlap),
        });
        f.ValidateDoesNotHaveYdbTables({
            "/Root/RestorePrefix/dir1/Table1",
            "/Root/RestorePrefix/dir1/dir2/Table2",
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportRecursiveWithoutDestinationPrefixImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    const TString prefixRaw = traits.FilePrefixRaw();
    const TString importPrefix = traits.ImportSrcPrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);
    if constexpr (std::is_same_v<TExportSettings, NExport::TExportToFsSettings>) {
        f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableEncryptedExport(false);
    }

    {
        auto exportSettings = traits.MakeExportSettingsRaw(f);
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1", .Dst = "Prefix"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefixRaw + "Prefix/Table1/metadata.json",
            prefixRaw + "Prefix/Table1/scheme.pb",
            prefixRaw + "Prefix/Table1/permissions.pb",
            prefixRaw + "Prefix/Table1/data_00.csv",
            prefixRaw + "Prefix/dir2/Table2/metadata.json",
            prefixRaw + "Prefix/dir2/Table2/scheme.pb",
            prefixRaw + "Prefix/dir2/Table2/permissions.pb",
            prefixRaw + "Prefix/dir2/Table2/data_00.csv",

            prefixRaw + "Prefix/Table1/metadata.json.sha256",
            prefixRaw + "Prefix/Table1/scheme.pb.sha256",
            prefixRaw + "Prefix/Table1/permissions.pb.sha256",
            prefixRaw + "Prefix/Table1/data_00.csv.sha256",
            prefixRaw + "Prefix/dir2/Table2/metadata.json.sha256",
            prefixRaw + "Prefix/dir2/Table2/scheme.pb.sha256",
            prefixRaw + "Prefix/dir2/Table2/permissions.pb.sha256",
            prefixRaw + "Prefix/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettingsNoDestination(f);
        auto res = traits.Import(f, importSettings);
        if constexpr (std::is_same_v<TExportSettings, NExport::TExportToS3Settings>) {
            f.WaitOpStatus(res, EStatus::CANCELLED);
        } else {
            f.WaitOpStatus(res, EStatus::BAD_REQUEST);
        }
    }

    {
        auto importSettings = traits.MakeImportSettingsRaw(f);
        importSettings
            .AppendItem(typename TImportSettings::TItem{.Src = "Prefix/Table1", .Dst = "/Root/RestorePrefix/Table11"})
            .AppendItem(typename TImportSettings::TItem{.Src = "Prefix/dir2/Table2", .Dst = "/Root/RestorePrefix/Table12"});
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table11", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table12", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportDirectoryWithEncryptionImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    const TString encExt = traits.EncryptedFileExtension();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1");
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json" + encExt,
            prefix + "SchemaMapping/mapping.json" + encExt,
            prefix + "001/metadata.json" + encExt,
            prefix + "001/scheme.pb" + encExt,
            prefix + "001/permissions.pb" + encExt,
            prefix + "001/data_00.csv" + encExt,
            prefix + "002/metadata.json" + encExt,
            prefix + "002/scheme.pb" + encExt,
            prefix + "002/permissions.pb" + encExt,
            prefix + "002/data_00.csv" + encExt,

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "001/metadata.json.sha256",
            prefix + "001/scheme.pb.sha256",
            prefix + "001/permissions.pb.sha256",
            prefix + "001/data_00.csv.sha256",
            prefix + "002/metadata.json.sha256",
            prefix + "002/scheme.pb.sha256",
            prefix + "002/permissions.pb.sha256",
            prefix + "002/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        traits.SetDecryptionKey(importSettings, "Cool random key!");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ExportWholeDatabaseWithEncryptionImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    const TString encExt = traits.EncryptedFileExtension();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "");
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json" + encExt,
            prefix + "SchemaMapping/mapping.json" + encExt,
            prefix + "001/metadata.json" + encExt,
            prefix + "001/scheme.pb" + encExt,
            prefix + "001/permissions.pb" + encExt,
            prefix + "001/data_00.csv" + encExt,
            prefix + "002/metadata.json" + encExt,
            prefix + "002/scheme.pb" + encExt,
            prefix + "002/permissions.pb" + encExt,
            prefix + "002/data_00.csv" + encExt,
            prefix + "003/metadata.json" + encExt,
            prefix + "003/scheme.pb" + encExt,
            prefix + "003/permissions.pb" + encExt,
            prefix + "003/data_00.csv" + encExt,

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "001/metadata.json.sha256",
            prefix + "001/scheme.pb.sha256",
            prefix + "001/permissions.pb.sha256",
            prefix + "001/data_00.csv.sha256",
            prefix + "002/metadata.json.sha256",
            prefix + "002/scheme.pb.sha256",
            prefix + "002/permissions.pb.sha256",
            prefix + "002/data_00.csv.sha256",
            prefix + "003/metadata.json.sha256",
            prefix + "003/scheme.pb.sha256",
            prefix + "003/permissions.pb.sha256",
            prefix + "003/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        traits.SetDecryptionKey(importSettings, "Cool random key!");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void EncryptedExportWithExplicitDestinationPathImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    const TString encExt = traits.EncryptedFileExtension();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "Table0", .Dst = "UnsafeTableNameShownInEncryptedBackup"})
            .AppendItem(typename TExportSettings::TItem{.Src = "dir1", .Dst = "Dir1Prefix"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json" + encExt,
            prefix + "SchemaMapping/mapping.json" + encExt,
            prefix + "UnsafeTableNameShownInEncryptedBackup/metadata.json" + encExt,
            prefix + "UnsafeTableNameShownInEncryptedBackup/scheme.pb" + encExt,
            prefix + "UnsafeTableNameShownInEncryptedBackup/permissions.pb" + encExt,
            prefix + "UnsafeTableNameShownInEncryptedBackup/data_00.csv" + encExt,
            prefix + "Dir1Prefix/Table1/metadata.json" + encExt,
            prefix + "Dir1Prefix/Table1/scheme.pb" + encExt,
            prefix + "Dir1Prefix/Table1/permissions.pb" + encExt,
            prefix + "Dir1Prefix/Table1/data_00.csv" + encExt,
            prefix + "Dir1Prefix/dir2/Table2/metadata.json" + encExt,
            prefix + "Dir1Prefix/dir2/Table2/scheme.pb" + encExt,
            prefix + "Dir1Prefix/dir2/Table2/permissions.pb" + encExt,
            prefix + "Dir1Prefix/dir2/Table2/data_00.csv" + encExt,

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "UnsafeTableNameShownInEncryptedBackup/metadata.json.sha256",
            prefix + "UnsafeTableNameShownInEncryptedBackup/scheme.pb.sha256",
            prefix + "UnsafeTableNameShownInEncryptedBackup/permissions.pb.sha256",
            prefix + "UnsafeTableNameShownInEncryptedBackup/data_00.csv.sha256",
            prefix + "Dir1Prefix/Table1/metadata.json.sha256",
            prefix + "Dir1Prefix/Table1/scheme.pb.sha256",
            prefix + "Dir1Prefix/Table1/permissions.pb.sha256",
            prefix + "Dir1Prefix/Table1/data_00.csv.sha256",
            prefix + "Dir1Prefix/dir2/Table2/metadata.json.sha256",
            prefix + "Dir1Prefix/dir2/Table2/scheme.pb.sha256",
            prefix + "Dir1Prefix/dir2/Table2/permissions.pb.sha256",
            prefix + "Dir1Prefix/dir2/Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        traits.SetDecryptionKey(importSettings, "Cool random key!");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir1/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void EncryptedExportWithExplicitObjectListImpl(TBackupTestFixture& f, bool isOlap) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    const TString encExt = traits.EncryptedFileExtension();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        // Export with encryption with explicitly specifying objects list (without common prefix first - error)
        auto exportSettings = traits.MakeExportSettingsRaw(f);
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/Table0", .Dst = "Table0"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/Table1", .Dst = "Table1"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2"});
        if constexpr (std::is_same_v<TExportSettings, NExport::TExportToS3Settings>) {
            auto res = traits.Export(f, exportSettings);
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST,
                "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        }
    }

    {
        // Now with common prefix - should work
        auto exportSettings = traits.MakeExportSettings(f, "");
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        exportSettings
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/Table0"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/Table1"})
            .AppendItem(typename TExportSettings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/Table2"});
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json" + encExt,
            prefix + "SchemaMapping/mapping.json" + encExt,
            prefix + "001/metadata.json" + encExt,
            prefix + "001/scheme.pb" + encExt,
            prefix + "001/permissions.pb" + encExt,
            prefix + "001/data_00.csv" + encExt,
            prefix + "002/metadata.json" + encExt,
            prefix + "002/scheme.pb" + encExt,
            prefix + "002/permissions.pb" + encExt,
            prefix + "002/data_00.csv" + encExt,
            prefix + "003/metadata.json" + encExt,
            prefix + "003/scheme.pb" + encExt,
            prefix + "003/permissions.pb" + encExt,
            prefix + "003/data_00.csv" + encExt,

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "001/metadata.json.sha256",
            prefix + "001/scheme.pb.sha256",
            prefix + "001/permissions.pb.sha256",
            prefix + "001/data_00.csv.sha256",
            prefix + "002/metadata.json.sha256",
            prefix + "002/scheme.pb.sha256",
            prefix + "002/permissions.pb.sha256",
            prefix + "002/data_00.csv.sha256",
            prefix + "003/metadata.json.sha256",
            prefix + "003/scheme.pb.sha256",
            prefix + "003/permissions.pb.sha256",
            prefix + "003/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestorePrefix");
        traits.SetDecryptionKey(importSettings, "Cool random key!");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        f.ValidateHasYdbPaths({
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", isOlap),
            TBackupTestFixture::TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", isOlap),
        });
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void EncryptedImportWithoutCommonPrefixImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    const TString prefix = traits.FilePrefix();
    const TString encExt = traits.EncryptedFileExtension();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    {
        auto exportSettings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing");
        traits.SetEncryption(exportSettings, NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        auto res = traits.Export(f, exportSettings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "SchemaMapping/metadata.json" + encExt,
            prefix + "SchemaMapping/mapping.json" + encExt,
            prefix + "001/metadata.json" + encExt,
            prefix + "001/scheme.pb" + encExt,
            prefix + "001/permissions.pb" + encExt,
            prefix + "001/data_00.csv" + encExt,
            prefix + "002/metadata.json" + encExt,
            prefix + "002/scheme.pb" + encExt,
            prefix + "002/permissions.pb" + encExt,
            prefix + "002/data_00.csv" + encExt,
            prefix + "003/metadata.json" + encExt,
            prefix + "003/scheme.pb" + encExt,
            prefix + "003/permissions.pb" + encExt,
            prefix + "003/data_00.csv" + encExt,

            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "001/metadata.json.sha256",
            prefix + "001/scheme.pb.sha256",
            prefix + "001/permissions.pb.sha256",
            prefix + "001/data_00.csv.sha256",
            prefix + "002/metadata.json.sha256",
            prefix + "002/scheme.pb.sha256",
            prefix + "002/permissions.pb.sha256",
            prefix + "002/data_00.csv.sha256",
            prefix + "003/metadata.json.sha256",
            prefix + "003/scheme.pb.sha256",
            prefix + "003/permissions.pb.sha256",
            prefix + "003/data_00.csv.sha256",
        });
    }

    {
        // Encrypted import without common prefix should fail
        auto importSettings = traits.MakeImportSettingsRaw(f);
        traits.SetDecryptionKey(importSettings, "Cool random key!");
        importSettings
            .AppendItem(typename TImportSettings::TItem{.Src = traits.ImportSrcPrefix() + "001", .Dst = "/Root/RestorePrefix/Table0"});
        auto res = traits.Import(f, importSettings);
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST,
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ParallelBackupWholeDatabaseImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    using TImportSettings = typename TBackupTraits<TExportSettings>::TImportSettings;
    using TExportResponse = typename TBackupTraits<TExportSettings>::TExportResponse;
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);
    using namespace fmt::literals;
    {
        auto res = f.YdbQueryClient().ExecuteQuery(R"sql(
            INSERT INTO `/Root/RecursiveFolderProcessing/Table0` (key) VALUES (1);
            INSERT INTO `/Root/RecursiveFolderProcessing/dir1/Table1` (key) VALUES (2);
            INSERT INTO `/Root/RecursiveFolderProcessing/dir1/dir2/Table2` (key) VALUES (3);
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    // Check that backup process does not export directories created by parallel export (/Root/export-123)
    constexpr size_t parallelExportsCount = 5;
    {
        std::vector<NThreading::TFuture<TExportResponse>> parallelBackups(parallelExportsCount);

        // Start parallel backups
        // They are expected not to export special export copies of tables (/Root/export-123), and also ".sys" and ".metadata" folders
        for (size_t i = 0; i < parallelBackups.size(); ++i) {
            auto& backupOp = parallelBackups[i];
            TExportSettings settings = traits.MakeExportSettingsCustomDest(f, "", TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i);
            backupOp = traits.ExportAsync(f, settings);
        }

        // Wait
        for (auto& backupOp : parallelBackups) {
            f.WaitOpSuccess(backupOp.GetValueSync());
        }

        // Forget
        for (auto& backupOp : parallelBackups) {
            auto forgetResult = f.YdbOperationClient().Forget(backupOp.GetValueSync().Id()).GetValueSync();
            UNIT_ASSERT_C(forgetResult.IsSuccess(), forgetResult.GetIssues().ToString());
        }
    }

    for (size_t i = 0; i < parallelExportsCount; ++i) {
        TImportSettings settings = traits.MakeImportSettingsCustomSource(f, TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i, TStringBuilder() << "/Root/Restored_" << i);

        const auto restoreOp = traits.Import(f, settings);
        f.WaitOpSuccess(restoreOp);

        // Check that there are only expected tables
        auto checkOneTableInDirectory = [&](const TString& dir, const TString& name) {
            auto listResult = f.YdbSchemeClient().ListDirectory(TStringBuilder() << "/Root/Restored_" << i << "/" << dir).GetValueSync();
            UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
            size_t tablesFound = 0;
            size_t tableIndex = 0;
            for (size_t i = 0; i < listResult.GetChildren().size(); ++i) {
                const auto& child = listResult.GetChildren()[i];
                if (child.Type == NYdb::NScheme::ESchemeEntryType::Table || child.Type == NYdb::NScheme::ESchemeEntryType::ColumnTable) {
                    ++tablesFound;
                    tableIndex = i;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(tablesFound, 1, "Current directory \"/Root/Restored_" << i << "/" << dir << "\" children: " << f.DebugListDir(TStringBuilder() << "/Root/Restored_" << i << "/" << dir));
            UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[tableIndex].Name, name);
        };
        checkOneTableInDirectory("RecursiveFolderProcessing", "Table0");
        checkOneTableInDirectory("RecursiveFolderProcessing/dir1", "Table1");
        checkOneTableInDirectory("RecursiveFolderProcessing/dir1/dir2", "Table2");
    }

    // Test restore to database root
    {
        // Remove all contents from database
        auto removeTable = [&](const TString& path) {
            auto session = f.YdbTableClient().GetSession().GetValueSync();
            UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
            auto res = session.GetSession().DropTable(path).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), "Drop table \"" << path << "\" failed: " << res.GetIssues().ToString());
        };
        auto removeDirectory = [&](const TString& path, bool ignoreErrors = false) {
            auto res = f.YdbSchemeClient().RemoveDirectory(path).GetValueSync();
            UNIT_ASSERT_C(ignoreErrors || res.IsSuccess(), "Drop directory \"" << path << "\" failed: " << res.GetIssues().ToString() << ". Current directory children: " << f.DebugListDir(path));
        };
        auto remove = [&](const TString& root, bool removeRoot = true) {
            removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/Table0");
            removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/Table1");
            removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2/Table2");
            removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2/dir3", true); // We don't restore empty dirs
            removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2");
            removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1");
            removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing");
            if (removeRoot) {
                removeDirectory(root);
            }
        };
        for (size_t i = 0; i < parallelExportsCount; ++i) {
            remove(TStringBuilder() << "/Root/Restored_" << i);
        }
        remove("/Root", false);
        auto listResult = f.YdbSchemeClient().ListDirectory("/Root").GetValueSync();
        UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetChildren().size(), 2, "Current database directory children: " << f.DebugListDir("/Root")); // .sys, .metadata

        // Import to database root
        TImportSettings settings = traits.MakeImportSettingsCustomSource(f, "ParallelBackupWholeDatabasePrefix_0", "");

        const auto restoreOp = traits.Import(f, settings);
        f.WaitOpSuccess(restoreOp);

        // Check data
        auto checkTableData = [&](const TString& path, ui32 data) {
            auto result = f.YdbQueryClient().ExecuteQuery(
                fmt::format(R"sql(
                    SELECT key FROM `{table_path}`;
                )sql",
                "table_path"_a = path
                ),
                NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto resultSet = result.GetResultSetParser(0);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint32(), data);
        };

        checkTableData("/Root/RecursiveFolderProcessing/Table0", 1);
        checkTableData("/Root/RecursiveFolderProcessing/dir1/Table1", 2);
        checkTableData("/Root/RecursiveFolderProcessing/dir1/dir2/Table2", 3);
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void ChecksumsForSchemaMappingFilesImpl(TBackupTestFixture& f, bool /*isOlap*/) {
    TBackupTraits<TExportSettings> traits;
    const TString prefix = traits.FilePrefix();
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);
    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);

    {
        auto settings = traits.MakeExportSettings(f, "/Root/RecursiveFolderProcessing/dir1/dir2");
        traits.SetCompression(settings, "zstd");

        auto res = traits.Export(f, settings);
        f.WaitOpSuccess(res);

        traits.ValidateFileList(f, {
            prefix + "metadata.json",
            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table2/metadata.json",
            prefix + "Table2/metadata.json.sha256",
            prefix + "Table2/scheme.pb",
            prefix + "Table2/scheme.pb.sha256",
            prefix + "Table2/permissions.pb",
            prefix + "Table2/permissions.pb.sha256",
            prefix + "Table2/data_00.csv.zst",
            prefix + "Table2/data_00.csv.sha256",
        });
    }

    {
        auto importSettings = traits.MakeImportSettings(f, "/Root/RestoredPath");
        auto res = traits.Import(f, importSettings);
        f.WaitOpSuccess(res);

        traits.ModifyChecksumAndCheckThatImportFails(f, {
            prefix + "metadata.json.sha256",
            prefix + "SchemaMapping/metadata.json.sha256",
            prefix + "SchemaMapping/mapping.json.sha256",
            prefix + "Table2/metadata.json.sha256",
            prefix + "Table2/scheme.pb.sha256",
            prefix + "Table2/data_00.csv.sha256",
        }, importSettings);
    }
}

template <typename TExportSettings, typename TBackupTestFixture>
void CancelWhileProcessingImpl(TBackupTestFixture& f, bool isOlap) {
    using namespace fmt::literals;
    TBackupTraits<TExportSettings> traits;

    f.Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);

    if (isOlap) {
        return;
    }

    auto createSchemaResult = f.YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
        CREATE TABLE `/Root/Table0` (
            key Uint32 NOT NULL,
            value String,
            PRIMARY KEY (key)
        ) WITH (
            STORE = {store}
            {partition_count}
        );

        CREATE TABLE `/Root/Table1` (
            key Uint32 NOT NULL,
            value String,
            PRIMARY KEY (key)
        ) WITH (
            STORE = {store}
            {partition_count}
        );

        CREATE TABLE `/Root/Table2` (
            key Uint32 NOT NULL,
            value String,
            PRIMARY KEY (key)
        ) WITH (
            STORE = {store}
            {partition_count}
        );

        CREATE TABLE `/Root/Table3` (
            key Uint32 NOT NULL,
            value String,
            PRIMARY KEY (key)
        ) WITH (
            STORE = {store}
            {partition_count}
        );

        CREATE TABLE `/Root/Table4` (
            key Uint32 NOT NULL,
            value String,
            PRIMARY KEY (key)
        ) WITH (
            STORE = {store}
            {partition_count}
        );
    )sql", "store"_a = isOlap ? "COLUMN" : "ROW",
    "partition_count"_a = isOlap ? ", PARTITION_COUNT = 1" : ""), NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(createSchemaResult.IsSuccess(), createSchemaResult.GetIssues().ToString());

    for (bool cancelExport : {true, false}) {
        TString dest = TStringBuilder() << "Prefix_" << cancelExport;
        auto exportSettings = traits.MakeExportSettingsCustomDest(f, "", dest);
        auto exportResult = traits.Export(f, exportSettings);
        if (cancelExport) {
            Sleep(TDuration::MilliSeconds(RandomNumber<ui64>(1500)));
            f.YdbOperationClient().Cancel(exportResult.Id()).GetValueSync();
            f.WaitOpStatus(exportResult, {NYdb::EStatus::SUCCESS, NYdb::EStatus::CANCELLED});
            continue;
        }
        f.WaitOpSuccess(exportResult);

        auto importSettings = traits.MakeImportSettingsCustomSource(f, dest, "/Root/RestorePrefix");
        auto importResult = traits.Import(f, importSettings);

        Sleep(TDuration::MilliSeconds(RandomNumber<ui64>(1500)));
        f.YdbOperationClient().Cancel(importResult.Id()).GetValueSync();
        f.WaitOpStatus(importResult, {NYdb::EStatus::SUCCESS, NYdb::EStatus::CANCELLED});
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE_F(BackupPathTestFs, TBackupPathTestFixtureFs) {
    Y_UNIT_TEST(ImportFilterByYdbObjectPath) {
        ImportFilterByYdbObjectPathImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExplicitDuplicatedItems) {
        ExplicitDuplicatedItemsImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportUnexistingExplicitPath) {
        ExportUnexistingExplicitPathImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportUnexistingCommonSourcePath) {
        ExportUnexistingCommonSourcePathImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(OnlyOneEmptyDirectory) {
        OnlyOneEmptyDirectoryImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportWholeDatabase) {
        ExportWholeDatabaseImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportWithCommonSourcePath) {
        ExportWithCommonSourcePathImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportWithExcludeRegexps) {
        ExportWithExcludeRegexpsImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportWithCommonSourcePathAndExplicitTableInside) {
        ExportWithCommonSourcePathAndExplicitTableInsideImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(RecursiveDirectoryPlusExplicitTable) {
        RecursiveDirectoryPlusExplicitTableImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(EmptyDirectoryIsOk) {
        EmptyDirectoryIsOkImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ImportWithExcludeRegexps) {
        ImportWithExcludeRegexpsImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ImportFilterByPrefix) {
        ImportFilterByPrefixImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(CommonPrefixButExplicitImportItems) {
        CommonPrefixButExplicitImportItemsImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportCommonSourcePathImportExplicitly) {
        ExportCommonSourcePathImportExplicitlyImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportRecursiveWithoutDestinationPrefix) {
        ExportRecursiveWithoutDestinationPrefixImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(ExportWholeDatabaseWithEncryption) {
        ExportWholeDatabaseWithEncryptionImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(EncryptedExportWithExplicitDestinationPath) {
        EncryptedExportWithExplicitDestinationPathImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(EncryptedExportWithExplicitObjectList) {
        EncryptedExportWithExplicitObjectListImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    // Y_UNIT_TEST(ParallelBackupWholeDatabase) {
    //     ParallelBackupWholeDatabaseImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    // }
    Y_UNIT_TEST(ChecksumsForSchemaMappingFiles) {
        ChecksumsForSchemaMappingFilesImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
    Y_UNIT_TEST(CancelWhileProcessing) {
        CancelWhileProcessingImpl<NExport::TExportToFsSettings, TFsBackupTestFixture>(*this, false);
    }
}

Y_UNIT_TEST_SUITE_F(BackupPathTest, TBackupPathTestFixture) {
    Y_UNIT_TEST_TWIN(ExportWholeDatabase, IsOlap) {
        ExportWholeDatabaseImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportWholeDatabaseWithEncryption, IsOlap) {
        ExportWholeDatabaseWithEncryptionImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportWithCommonSourcePath, IsOlap) {
        ExportWithCommonSourcePathImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportWithExcludeRegexps, IsOlap) {
        ExportWithExcludeRegexpsImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ImportWithExcludeRegexps, IsOlap) {
        ImportWithExcludeRegexpsImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportWithCommonSourcePathAndExplicitTableInside, IsOlap) {
        ExportWithCommonSourcePathAndExplicitTableInsideImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(RecursiveDirectoryPlusExplicitTable, IsOlap) {
        RecursiveDirectoryPlusExplicitTableImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(EmptyDirectoryIsOk, IsOlap) {
        EmptyDirectoryIsOkImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(CommonPrefixButExplicitImportItems, IsOlap) {
        CommonPrefixButExplicitImportItemsImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportDirectoryWithEncryption, IsOlap) {
        ExportDirectoryWithEncryptionImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(EncryptedExportWithExplicitDestinationPath, IsOlap) { // supported, but not recommended
        EncryptedExportWithExplicitDestinationPathImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(EncryptedExportWithExplicitObjectList, IsOlap) {
        EncryptedExportWithExplicitObjectListImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportCommonSourcePathImportExplicitly, IsOlap) {
        ExportCommonSourcePathImportExplicitlyImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ImportFilterByPrefix, IsOlap) {
        ImportFilterByPrefixImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ImportFilterByYdbObjectPath, IsOlap) {
        ImportFilterByYdbObjectPathImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(EncryptedImportWithoutCommonPrefix, IsOlap) {
        EncryptedImportWithoutCommonPrefixImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExplicitDuplicatedItems, IsOlap) {
        ExplicitDuplicatedItemsImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportUnexistingExplicitPath, IsOlap) {
        ExportUnexistingExplicitPathImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportUnexistingCommonSourcePath, IsOlap) {
        ExportUnexistingCommonSourcePathImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(FilterByPathFailsWhenNoSchemaMapping, IsOlap) {
        FilterByPathFailsWhenNoSchemaMappingImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(OnlyOneEmptyDirectory, IsOlap) {
        OnlyOneEmptyDirectoryImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ExportRecursiveWithoutDestinationPrefix, IsOlap) {
        ExportRecursiveWithoutDestinationPrefixImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ParallelBackupWholeDatabase, IsOlap) {
        ParallelBackupWholeDatabaseImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    Y_UNIT_TEST_TWIN(ChecksumsForSchemaMappingFiles, IsOlap) {
        ChecksumsForSchemaMappingFilesImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }

    // Test that covers races between processing and cancellation
    Y_UNIT_TEST_TWIN(CancelWhileProcessing, IsOlap) {
        CancelWhileProcessingImpl<NExport::TExportToS3Settings, TS3BackupTestFixture>(*this, IsOlap);
    }
}
