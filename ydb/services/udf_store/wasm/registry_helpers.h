#pragma once

#include "registry.h"

#include <ydb/library/wasm/api/bytecode.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;
using EAbiValueFlags = NYdb::NUdfStore::NAbi::EValueFlags;

EUdfValueType ParseValueType(TStringBuf type);

const char* ValueTypeToString(EUdfValueType type);

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

TUnversionedValue MakeEmptyValue();

//! TUnversionedValue.Length is ui32 — reject oversized host strings.
ui32 CheckedAbiLength(size_t size, TStringBuf what);

void StoreValue(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    uintptr_t offset,
    const TUnversionedValue& value);

struct TPreparedUdfArg {
    NYdb::NWasm::TCopyGuard ValueGuard;
    NYdb::NWasm::TCopyGuard StringGuard;
    uintptr_t Offset = 0;
};

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

THashSet<TString> CollectWasmExports(
    TStringBuf bytes,
    NYdb::NWasm::EBytecodeFormat format);

NYdb::NWasm::TModuleBytecode MakeModuleBytecode(
    TStringBuf wasmData,
    TStringBuf objectCode,
    NYdb::NWasm::EBytecodeFormat format);

} // namespace NKikimr::NUdfStore::NWasm
