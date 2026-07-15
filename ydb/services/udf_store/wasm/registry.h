#pragma once

#include "types.h"

#include <ydb/library/wasm/api/compartment.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <mutex>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmCompartmentState {
    std::mutex InvocationMutex;
    std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> Compartment;
    THashMap<TString, TWasmUdfDescriptor> Functions;
    TVector<TString> FunctionOrder;
    THashSet<TString> Exports;
    TString Md5;
    TString ModuleName;
};

using TWasmCompartmentStatePtr = std::shared_ptr<TWasmCompartmentState>;

} // namespace NKikimr::NUdfStore::NWasm
