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

bool TYqlExternalModuleProcessor::ApplyConfigFlag(const TPosition& pos, const TString& flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) {
    for (auto& [name, ptr] : KnownModules) {
        if (!ptr->ApplyConfigFlag(pos, flagName, ctx, args, crutches)) {
            return false;
        }
    }
    return true;
}

TVector<IYqlModule::TDatafileTraits> TYqlExternalModuleProcessor::GetUsedFilenamePaths(const TString& moduleName) {
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

const IYqlModule* TYqlExternalModuleProcessor::GetModule(const TString& name) {
    return KnownModules.contains(name) ? KnownModules.at(name) : nullptr;
}

} // NYql
