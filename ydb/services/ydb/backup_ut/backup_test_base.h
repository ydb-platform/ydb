#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/maybe.h>

#define YDB_SDK_CLIENT(type, funcName)                               \
    protected:                                                       \
    TMaybe<type> Y_CAT(funcName, Instance);                          \
    public:                                                          \
    type& funcName() {                                               \
        if (!Y_CAT(funcName, Instance)) {                            \
            Y_CAT(funcName, Instance).ConstructInPlace(YdbDriver()); \
        }                                                            \
        return *Y_CAT(funcName, Instance);                           \
    }                                                                \
    /**/

template <typename TDerived, typename TExportSettings, typename TImportSettings>
class TBackupTestFixture : public NUnitTest::TBaseFixture {
public:
    TExportSettings MakeExportSettings(const TString& sourcePath, const TString& destinationPrefix) {
        return static_cast<TDerived*>(this)->MakeExportSettings(sourcePath, destinationPrefix);
    }

    TImportSettings MakeImportSettings(const TString& sourcePrefix, const TString& destinationPath) {
        return static_cast<TDerived*>(this)->MakeImportSettings(sourcePrefix, destinationPath);
    }

    const auto& Export(const TExportSettings& settings) {
        return static_cast<TDerived*>(this)->Export(settings);
    }

    const auto& Import(const TImportSettings& settings) {
        return static_cast<TDerived*>(this)->Import(settings);
    }

    void ValidateFileList(const TSet<TString>& paths) {
        static_cast<TDerived*>(this)->ValidateFileList(paths);
    }
};
