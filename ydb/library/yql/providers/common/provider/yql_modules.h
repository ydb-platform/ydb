#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>  // TUploadList

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

class IYqlModule {
public:
    virtual ~IYqlModule() {
    }

    virtual const TString& GetName() const = 0;
    virtual bool IsKnownPragma(const TString& checkedPragmaName) const = 0;

    virtual void FillUsedFiles(const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files) const = 0;
    virtual bool ApplyConfigFlag(const TString& flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) const = 0;
    virtual void TuneUploadList(TUserDataTable& files, IDqGateway::TUploadList* uploadList) const = 0;
    virtual TVector<TString> GetUsedFilenamePaths() const = 0;
    virtual void PragmaProcessing(const TYtSettings::TConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches) const = 0;
};

class TYqlExternalModuleProcessor {
public:
    static void AddModule(const IYqlModule* ptr);

    static void FillUsedFiles(const TString& moduleName, const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files);
    static bool ApplyConfigFlag(const TString& flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches);
    static void TuneUploadList(const TString& moduleName, TUserDataTable& files, IDqGateway::TUploadList* uploadList);
    static TVector<TString> GetUsedFilenamePaths(const TString& moduleName);
    static void PragmaProcessing(const TYtSettings::TConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches);

private:
    static const IYqlModule* GetModule(const TString& name);

private:
    static THashMap<TString, const IYqlModule*> KnownModules;
};

} // NYql
