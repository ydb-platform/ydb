#include "single_module_loader.h"

#include "module_catalog.h"
#include "registry_helpers.h"
#include "types.h"

#include <util/generic/yexception.h>

namespace NKikimr::NUdfStore::NWasm {

TWasmModuleStatePtr BuildModuleStateFromManifest(const TWasmLoadParams& params) {
    if (params.ModuleObjectCode.empty()) {
        ythrow yexception()
            << "Precompiled object code is required for WASM UDF '"
            << params.Manifest.ModuleName << "'";
    }

    auto artifact = std::make_shared<TWasmModuleArtifact>();
    artifact->ModuleName = params.Manifest.ModuleName;
    artifact->Manifest = params.Manifest;
    artifact->ModuleBytecode = MakeModuleBytecode(
        params.ModuleWasmData,
        params.ModuleObjectCode,
        params.ModuleFormat);
    artifact->Libraries = params.Libraries;
    GetWasmModuleCatalog().Register(artifact);

    auto state = std::make_shared<TWasmModuleState>();
    state->ModuleName = params.Manifest.ModuleName;
    for (const auto& descriptor : params.Manifest.Functions) {
        state->Functions[descriptor.Name] = descriptor;
        state->FunctionOrder.push_back(descriptor.Name);
        if (descriptor.Binding == EWasmUdfBinding::TypeConfigCallable) {
            state->Exports.insert(descriptor.CreateExport);
            state->Exports.insert(descriptor.CallExport);
            if (!descriptor.DestroyExport.empty()) {
                state->Exports.insert(descriptor.DestroyExport);
            }
        } else {
            state->Exports.insert(TString(PlainWasmExport(descriptor)));
        }
    }
    return state;
}

TWasmCompartmentStatePtr LoadWasmFromManifest(const TWasmLoadParams& params) {
    return BuildModuleStateFromManifest(params);
}

} // namespace NKikimr::NUdfStore::NWasm

