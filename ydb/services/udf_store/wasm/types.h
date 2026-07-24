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

enum class EWasmUdfBinding {
    Plain,
    TypeConfigCallable,
};

struct TWasmUdfDescriptor {
    TString Name;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
    EWasmUdfBinding Binding = EWasmUdfBinding::Plain;
    // For TypeConfigCallable: create/call/destroy exports (destroy optional).
    TString CreateExport;
    TString CallExport;
    TString DestroyExport;
};

struct TWasmObjectMethodDescriptor {
    TString Name;
    TString Export;
    EWasmUdfBinding Binding = EWasmUdfBinding::TypeConfigCallable;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
};

struct TWasmObjectDescriptor {
    TString Name;
    TString CreateExport;
    TString DestroyExport;
    TVector<TWasmObjectMethodDescriptor> Methods;
};

struct TWasmManifest {
    TString ModuleName;
    TString ModuleExtension;
    TString CallingConvention;
    TVector<TString> RequiredLibraries;
    TVector<TWasmUdfDescriptor> Functions;
    TVector<TWasmObjectDescriptor> Objects;
};

} // namespace NKikimr::NUdfStore::NWasm
