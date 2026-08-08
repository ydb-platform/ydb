#pragma once

#include "types.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmModuleState {
    THashMap<TString, TWasmUdfDescriptor> Functions;
    TVector<TString> FunctionOrder;
    THashSet<TString> Exports;
    TString Md5;
    TString ModuleName;
};

using TWasmModuleStatePtr = std::shared_ptr<TWasmModuleState>;

// Compatibility aliases while call sites migrate.
using TWasmCompartmentState = TWasmModuleState;
using TWasmCompartmentStatePtr = TWasmModuleStatePtr;

} // namespace NKikimr::NUdfStore::NWasm
