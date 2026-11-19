#pragma once

#include "helpers.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <library/cpp/json/json_reader.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NSchemeShardUT_Private {

using NKikimr::NWrappers::NTestHelpers::TS3Mock;

inline NJson::TJsonValue ParseCompletionMarker(const TString& content) {
    NJson::TJsonValue marker;
    UNIT_ASSERT_C(NJson::ReadJsonTree(content, &marker), content);
    UNIT_ASSERT_C(marker.IsMap(), content);
    UNIT_ASSERT_C(marker["version"].IsInteger(), content);
    UNIT_ASSERT_VALUES_EQUAL(marker["version"].GetInteger(), 1);
    UNIT_ASSERT_C(marker["export_id"].IsString(), content);
    return marker;
}

class TBackupTestFixture : public NUnitTest::TBaseFixture {
public:
    bool HasS3File(const TString& path) {
        auto it = S3Mock().GetData().find(path);
        return it != S3Mock().GetData().end();
    }

    TString GetS3FileContent(const TString& path) {
        auto it = S3Mock().GetData().find(path);
        if (it != S3Mock().GetData().end()) {
            return it->second;
        }
        return {};
    }

    ui64 StartCompletionMarkerExport(bool autoDropping = true, bool withRows = false, bool withChangefeed = false) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableExportAutoDropping(autoDropping);
        TestCreateTable(Runtime(), 100, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )");
        Env().TestWaitNotification(Runtime(), 100);
        if (withRows) {
            UpdateRow(Runtime(), "Table", 1, "first");
            UpdateRow(Runtime(), "Table", 2, "second");
        }
        const ui64 exportId = withChangefeed ? 102 : 101;
        if (withChangefeed) {
            Runtime().GetAppData().FeatureFlags.SetEnableChangefeedsExport(true);
            TestCreateCdcStream(Runtime(), TTestTxConfig::SchemeShard, 101, "/MyRoot", R"(
                    TableName: "Table"
                    StreamDescription {
                        Name: "stream"
                        Mode: ECdcStreamModeUpdate
                        Format: ECdcStreamFormatJson
                        State: ECdcStreamStateReady
                    }
                )");
            Env().TestWaitNotification(Runtime(), 101);
        }
        TestExport(Runtime(), exportId, "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                    endpoint: "localhost:%d"
                    scheme: HTTP
                    number_of_retries: 2
                    items { source_path: "/MyRoot/Table" destination_prefix: "backup/table" }
                }
            )", S3Port()));
        return exportId;
    }

    void CheckCompletionMarkerContents(ui64 exportId) {
        for (const auto* path : { "/backup/table/data_complete.json", "/backup/table/export_complete.json" }) {
            const auto marker = ParseCompletionMarker(GetS3FileContent(path));
            UNIT_ASSERT_VALUES_EQUAL_C(marker["export_id"].GetString(), ToString(exportId), path);
        }
    }

    void TearDown(NUnitTest::TTestContext&) override {
        if (S3ServerMock) {
            S3ServerMock = Nothing();
            S3ServerPort = 0;
        }
    }

protected:
    TS3Mock::TSettings& S3Settings() {
        if (!S3ServerSettings) {
            S3ServerPort = PortManager.GetPort();
            S3ServerSettings.ConstructInPlace(S3ServerPort);
        }
        return *S3ServerSettings;
    }

    TS3Mock& S3Mock() {
        if (!S3ServerMock) {
            S3ServerMock.ConstructInPlace(S3Settings());
            UNIT_ASSERT(S3ServerMock->Start());
        }
        return *S3ServerMock;
    }

    ui16 S3Port() {
        S3Mock();
        return S3ServerPort;
    }

    TTestBasicRuntime& Runtime() {
        if (!TestRuntime) {
            TestRuntime.ConstructInPlace();
        }
        return *TestRuntime;
    }

    TTestEnvOptions& EnvOptions() {
        if (!TestEnvOptions) {
            TestEnvOptions.ConstructInPlace();
        }
        return *TestEnvOptions;
    }

    TTestEnv& Env() {
        if (!TestEnv) {
            TestEnv.ConstructInPlace(Runtime(), EnvOptions());
        }
        return *TestEnv;
    }

private:
    TPortManager PortManager;
    ui16 S3ServerPort = 0;
    TMaybe<TTestBasicRuntime> TestRuntime;
    TMaybe<TS3Mock::TSettings> S3ServerSettings;
    TMaybe<TS3Mock> S3ServerMock;
    TMaybe<TTestEnvOptions> TestEnvOptions;
    TMaybe<TTestEnv> TestEnv;
};

struct TImportItem {
    TString SourcePrefix;
    TString DestinationPath;
};

inline const TVector<TImportItem>& DefaultImportItems() {
    static const TVector<TImportItem> items = { { "", "/MyRoot/Table" } };
    return items;
}

inline TString MakeS3ImportRequestTpl(const TVector<TImportItem>& items, const TString& extraSettings = "") {
    TStringBuilder sb;
    sb << "ImportFromS3Settings { endpoint: \"localhost:%d\" scheme: HTTP ";
    if (extraSettings) {
        sb << extraSettings << " ";
    }
    for (const auto& item : items) {
        sb << "items { source_prefix: \"" << item.SourcePrefix << "\" destination_path: \"" << item.DestinationPath
           << "\" } ";
    }
    sb << "}";
    return sb;
}

inline TString MakeFsImportRequest(
    const TString& basePath, const TVector<TImportItem>& items, const TString& extraSettings = "")
{
    TStringBuilder sb;
    sb << "ImportFromFsSettings { base_path: \"" << basePath << "\" ";
    if (extraSettings) {
        sb << extraSettings << " ";
    }
    for (const auto& item : items) {
        sb << "items { source_path: \"" << item.SourcePrefix << "\" destination_path: \"" << item.DestinationPath
           << "\" } ";
    }
    sb << "}";
    return sb;
}

inline void WriteFilesToFs(const TString& basePath, const THashMap<TString, TString>& data) {
    for (const auto& [key, value] : data) {
        TStringBuf relKey(key);
        while (relKey.StartsWith('/')) {
            relKey.Skip(1);
        }
        TFsPath filePath = TFsPath(basePath) / TString(relKey);
        filePath.Parent().MkDirs();
        TFileOutput out(filePath.GetPath());
        out.Write(value);
        out.Finish();
    }
}

template <bool IsFs>
struct TImportEnv {
    TMaybe<TPortManager> PortManager;
    TMaybe<TS3Mock> S3MockInstance;
    TMaybe<TTempDir> TempDir;
    TString Request;

    TImportEnv(
        const THashMap<TString, TString>& data, const TVector<TImportItem>& items, const TString& extraSettings = "")
    {
        if constexpr (IsFs) {
            TempDir.ConstructInPlace();
            WriteFilesToFs(TempDir->Path(), data);
            Request = MakeFsImportRequest(TempDir->Path(), items, extraSettings);
        } else {
            PortManager.ConstructInPlace();
            ui16 port = PortManager->GetPort();
            S3MockInstance.ConstructInPlace(data, TS3Mock::TSettings(port));
            UNIT_ASSERT(S3MockInstance->Start());
            Request = Sprintf(MakeS3ImportRequestTpl(items, extraSettings).c_str(), port);
        }
    }

    void SetupRuntime(TTestActorRuntime& runtime) {
        if constexpr (IsFs) {
            runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        }
    }
};

} // namespace NSchemeShardUT_Private
