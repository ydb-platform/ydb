#include "yql_modules.h"

namespace NYql {

THashMap<TString, const IYqlModule*> TYqlExternalModuleProcessor::KnownModules;

void TYqlExternalModuleProcessor::AddModule(const IYqlModule* ptr) {
    if (!ptr) {
        KnownModules[ptr->GetName()] = ptr;
    }
}

bool TYqlExternalModuleProcessor::ApplyConfigFlag(const TPosition& pos, TStringBuf flagName, TExprContext& ctx, const TVector<TStringBuf>& args, TUserDataTable& crutches) {
    for (auto& [name, ptr] : KnownModules) {
        if (!ptr->ApplyConfigFlag(pos, flagName, ctx, args, crutches)) {
            return false;
        }
    }
    return true;
}

TVector<IYqlModule::TDatafileTraits> TYqlExternalModuleProcessor::GetUsedFilenamePaths(TStringBuf moduleName) {
    if (const auto modulePtr = GetModule(moduleName)) {
        return modulePtr->GetUsedFilenamePaths();
    }

    return {};
}

void TYqlExternalModuleProcessor::PragmaProcessing(TYtSettingsConstPtr settingsPtr, const TString& cluster, TUserDataTable& crutches) {
    for (auto& [name, ptr] : KnownModules) {
        ptr->PragmaProcessing(settingsPtr, cluster, crutches);
    }
}

const IYqlModule* TYqlExternalModuleProcessor::GetModule(TStringBuf name) {
    const TString nameStr{name};
    return KnownModules.contains(nameStr) ? KnownModules.at(nameStr) : nullptr;
}

} // NYql
