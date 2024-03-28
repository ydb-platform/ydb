#include "yql_modules.h"

namespace NYql {

THashMap<TString, const IYqlModule*> TYqlExternalModuleProcessor::KnownModules;

void TYqlExternalModuleProcessor::AddModule(const IYqlModule* ptr) {
    if (!ptr) {
        KnownModules[ptr->GetName()] = ptr;
    }
}

void TYqlExternalModuleProcessor::FillUsedFiles(const TString& moduleName, const TTypeAnnotationContext& types, const TUserDataTable& crutches, TUserDataTable& files)
{
    if (const auto modulePtr = GetModule(moduleName)) {
        modulePtr->FillUsedFiles(types, crutches, files);
    }
}

bool TYqlExternalModuleProcessor::ApplyConfigFlag(const TString& flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) {
    for (auto& [name, ptr] : KnownModules) {
        if (!ptr->ApplyConfigFlag(flagName, ctx, args, crutches)) {
            return false;
        }
    }
    return true;
}

void TYqlExternalModuleProcessor::TuneUploadList(const TString& moduleName, TUserDataTable& files, IDqGateway::TUploadList* uploadList) {
    if (const auto modulePtr = GetModule(moduleName)) {
        modulePtr->TuneUploadList(files, uploadList);
    }
}

TVector<TString> TYqlExternalModuleProcessor::GetUsedFilenamePaths(const TString& moduleName) {
    if (const auto modulePtr = GetModule(moduleName)) {
        return modulePtr->GetUsedFilenamePaths();
    }

    return {};
}

void TYqlExternalModuleProcessor::PragmaProcessing(const TYtSettings::TConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches) {
    for (auto& [name, ptr] : KnownModules) {
        ptr->PragmaProcessing(settingsPtr, cluster, crutches);
    }
}

const TYqlModule* TYqlExternalModuleProcessor::GetModule(const TString& name) {
    return KnownModules.contains(name) ? KnownModules.at(name) : nullptr;
}

} // NYql
