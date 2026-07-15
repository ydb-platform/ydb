#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

enum class EUdfValueType {
    Null,
    Int64,
    Uint64,
    Double,
    Boolean,
    String,
};

struct TWasmUdfDescriptor {
    TString Name;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
};

struct TWasmManifest {
    TString ModuleName;
    TString ModuleExtension;
    TString CallingConvention;
    TVector<TString> RequiredLibraries;
    TVector<TWasmUdfDescriptor> Functions;
};

} // namespace NKikimr::NUdfStore::NWasm
