#pragma once

#include "s3_backup_test_base.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

#include <util/folder/tempdir.h>

using namespace NYdb;

class TFsBackupTestFixture : public TYdbTestBaseFixture {
public:
    const TTempDir& GetTempDir() {
        return TempDir;
    }

    NExport::TExportToFsSettings MakeExportSettings(const TString& sourcePath) {
        NExport::TExportToFsSettings settings;
        settings.BasePath(TString(GetTempDir().Path()));
        if (sourcePath) {
            settings.SourcePath(sourcePath);
        }
        return settings;
    }

    NImport::TImportFromFsSettings MakeImportSettings(const TString& destinationPath) {
        NImport::TImportFromFsSettings settings;
        settings.BasePath(TString(GetTempDir().Path()));
        if (destinationPath) {
            settings.DestinationPath(destinationPath);
        }
        return settings;
    }

    void ValidateFileList(const TSet<TString>& paths) {
        TFsPath basePath(GetTempDir().Path());
        TSet<TString> actual;
        CollectFiles(basePath, basePath, actual);
        UNIT_ASSERT_VALUES_EQUAL(actual, paths);
    }

private:
    void CollectFiles(const TFsPath& dir, const TFsPath& base, TSet<TString>& result) {
        TVector<TString> children;
        dir.ListNames(children);
        for (const auto& name : children) {
            TFsPath child = dir / name;
            if (child.IsDirectory()) {
                CollectFiles(child, base, result);
            } else {
                result.insert(child.GetPath().substr(base.GetPath().size()));
            }
        }
    }

    TTempDir TempDir;
};
