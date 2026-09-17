#pragma once

#include "registry.h"

#include <ydb/library/wasm/api/bytecode.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

struct TNamedModuleBytecode {
    TString Name;
    NYdb::NWasm::TModuleBytecode Bytecode;
};

void AddPrecompiledModule(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    const NYdb::NWasm::TModuleBytecode& bytecode,
    TStringBuf name);

std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> CreateRegistryCompartment(
    const TVector<TNamedModuleBytecode>& libraries);

//! Bridge string builders use ui32 lengths; reject oversized host strings.
ui32 CheckedAbiLength(size_t size, TStringBuf what);

class TCurrentCompartmentGuard {
public:
    explicit TCurrentCompartmentGuard(NYdb::NWasm::IWebAssemblyCompartment* compartment);
    ~TCurrentCompartmentGuard();

    TCurrentCompartmentGuard(const TCurrentCompartmentGuard&) = delete;
    TCurrentCompartmentGuard& operator=(const TCurrentCompartmentGuard&) = delete;

private:
    NYdb::NWasm::IWebAssemblyCompartment* Previous_;
};

void InvokeUdfExport(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    void* runtimeFunction,
    const TString& functionNameForErrors,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args);

void InvokeUdfExport(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    const TString& functionName,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args);

enum class EWasmExportValueType: ui8 {
    I32,
    I64,
    F32,
    F64,
    Other,
};

const char* WasmExportValueTypeAsStr(EWasmExportValueType type);

//! Shape of an exported wasm function, enough to check that a manifest
//! declaration and the module it describes agree on which values move across
//! the call. Every UDF export is invoked as (i64...) -> (), because
//! InvokeUdfExport passes context, result pointer and arguments as UintPtr
//! and expects no result back.
struct TWasmExportSignature {
    size_t ParamCount = 0;
    size_t ResultCount = 0;
    TVector<EWasmExportValueType> ParamTypes;
};

THashMap<TString, TWasmExportSignature> CollectWasmExports(
    TStringBuf bytes,
    NYdb::NWasm::EBytecodeFormat format);

NYdb::NWasm::TModuleBytecode MakeModuleBytecode(
    TStringBuf wasmData,
    TStringBuf objectCode,
    NYdb::NWasm::EBytecodeFormat format);

} // namespace NKikimr::NUdfStore::NWasm
