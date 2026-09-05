#pragma once

#include "manifest.h"
#include "registry.h"
#include "registry_helpers.h"

#include <ydb/library/wasm/api/bytecode.h>

#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmLoadParams {
    TWasmManifest Manifest;
    TString ModuleWasmData;
    TString ModuleObjectCode;
    NYdb::NWasm::EBytecodeFormat ModuleFormat = NYdb::NWasm::EBytecodeFormat::Binary;
    TVector<TNamedModuleBytecode> Libraries;
};

TWasmModuleStatePtr BuildModuleStateFromManifest(const TWasmLoadParams& params);

// Registers artifact in ModuleCatalog and returns metadata for FunctionRegistry.
TWasmCompartmentStatePtr LoadWasmFromManifest(const TWasmLoadParams& params);

} // namespace NKikimr::NUdfStore::NWasm
