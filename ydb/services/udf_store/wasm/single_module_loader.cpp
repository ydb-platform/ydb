#include "single_module_loader.h"

#include "compile.h"
#include "registry_helpers.h"

#include <util/generic/yexception.h>

namespace NKikimr::NUdfStore::NWasm {

TWasmCompartmentStatePtr LoadWasmFromManifest(const TWasmLoadParams& params) {
    if (params.ModuleObjectCode.empty()) {
        ythrow yexception()
            << "Precompiled object code is required for WASM UDF '" << params.Md5 << "'";
    }

    auto state = std::make_shared<TWasmCompartmentState>();
    state->Md5 = params.Md5;
    state->ModuleName = params.Manifest.ModuleName;
    state->Compartment = CreateRegistryCompartment(params.Libraries);

    const auto moduleBytecode = MakeModuleBytecode(
        params.ModuleWasmData,
        params.ModuleObjectCode,
        params.ModuleFormat);
    AddPrecompiledModule(
        state->Compartment.get(),
        moduleBytecode,
        params.Manifest.ModuleName);

    for (const auto& descriptor : params.Manifest.Functions) {
        state->Exports.insert(descriptor.Name);
        state->Functions[descriptor.Name] = descriptor;
        state->FunctionOrder.push_back(descriptor.Name);
    }

    return state;
}

} // namespace NKikimr::NUdfStore::NWasm

