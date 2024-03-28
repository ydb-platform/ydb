#include "yql_modules.h"

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>

namespace NYql {

class TYqlGeoModule : public IYqlModule {
public:
    const TString& GetName() const override {
        return Name;
    }

    bool IsKnownPragma(const TString& checkedPragmaName) const override {
        return checkedPragmaName == DatafilePragmaName
            || checkedPragmaName == ConfigPragmaName;
    }

    TVector<TDatafileTraits> GetUsedFilenamePaths() const override {
        return {{DatafilePath, true}, {ConfigPath, false}};
    }

    void FillUsedFiles(const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files) const override {
        auto datafileProcessing = [&](TStringBuf filename) -> void {
            const auto fileKey = TUserDataKey::File(filename);
            if (const auto block = types.UserDataStorage->FindUserDataBlock(fileKey)) {
                files.emplace(fileKey, *block).first->second.Usage.Set(EUserDataBlockUsage::Path);
            } else {
                const auto it = crutches.find(fileKey);
                if (crutches.cend() != it) {
                    auto pragma = it->second;
                    types.UserDataStorage->AddUserDataBlock(fileKey, pragma);
                    files.emplace(fileKey, pragma).first->second.Usage.Set(EUserDataBlockUsage::Path);
                }
            }
        };

        datafileProcessing(DatafilePath);
        datafileProcessing(ConfigPath);
    }

    bool ApplyConfigFlag(const TPosition& pos, TStringBuf name, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) const override {
        if (name == DatafilePragmaName) {
            if (args.size() != 1) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Expected 1 argument, but got " << args.size()));
                return false;
            }
            AddUserDataBlock(DatafilePath, TString(args[0]), crutches);
        }
        else if (name == ConfigPragmaName) {
            if (args.size() == 1) {
                AddUserDataBlock(ConfigPath, TString(args[0]), crutches);
            }
        }
        return true;
    }

    void PragmaProcessing(const TYtSettings::TConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches) const override {
        if (const auto& defaultGeobase = settingsPtr->GeobaseDownloadUrl.Get(cluster)) {
            AddUserDataBlock(DatafilePath, *defaultGeobase, crutches);
        }
        if (const auto& geobaseConfig = settingsPtr->GeobaseConfigUrl.Get(cluster)) {
            AddUserDataBlock(ConfigPragmaName, *geobaseConfig, crutches);
        }
    }

private:
    void AddUserDataBlock(const TString& fname, const TString& value, TUserDataTable& crutches) const {
        auto& userDataBlock = (crutches[TUserDataKey::File(fname)] = TUserDataBlock{EUserDataType::URL, {}, value, {}, {}});
        userDataBlock.Usage.Set(EUserDataBlockUsage::Path);
    }

private:
    static const TString Name;
    static const TString DatafilePath;
    static const TString ConfigPath;

    static const TString DatafilePragmaName;
    static const TString ConfigPragmaName;
};

const TString TYqlGeoModule::Name("Geo");
const TString TYqlGeoModule::DatafilePath("/home/geodata6.bin");
const TString TYqlGeoModule::ConfigPath("/home/geodata.conf");

const TString TYqlGeoModule::DatafilePragmaName("GeobaseDownloadUrl");
const TString TYqlGeoModule::ConfigPragmaName("GeobaseConfigUrl");

} // NYql
