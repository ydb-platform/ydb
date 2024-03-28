#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NYql {

struct TYtSettings;
using TYtSettingsConstPtr = std::shared_ptr<const TYtSettings>;

class IYqlModule {
public:
    struct TDatafileTraits {
        TString Name;
        bool IsRequired;
    };

public:
    virtual ~IYqlModule() {
    }

    virtual const TString& GetName() const = 0;
    virtual bool IsKnownPragma(const TString& checkedPragmaName) const = 0;

    virtual void FillUsedFiles(const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files) const = 0;
    virtual bool ApplyConfigFlag(const TPosition& pos, TStringBuf flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) const = 0;
    virtual TVector<TDatafileTraits> GetUsedFilenamePaths() const = 0;
    virtual void PragmaProcessing(TYtSettingsConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches) const = 0;
};

class TYqlExternalModuleProcessor {
public:
    static void AddModule(const IYqlModule* ptr);

    static void FillUsedFiles(TStringBuf moduleName, const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files);
    static bool ApplyConfigFlag(const TPosition& pos, TStringBuf flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches);
    static TVector<IYqlModule::TDatafileTraits> GetUsedFilenamePaths(TStringBuf moduleName);
    static void PragmaProcessing(TYtSettingsConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches);

private:
    static const IYqlModule* GetModule(TStringBuf name);

private:
    static THashMap<TString, const IYqlModule*> KnownModules;
};

} // NYql
