#pragma once

#include "wasm_udf_registry.hpp"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <yql/essentials/udfs/common/wasm/abi/udf_cpp_abi.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NWasm::NYQL {

using EAbiValueType = NYT::NQueryClient::NUdf::EValueType;
using EAbiValueFlags = NYT::NQueryClient::NUdf::EValueFlags;

bool EndsWith(TStringBuf value, TStringBuf suffix);

TString ReadFileContent(const TString& path);

TString JoinPath(const TString& directory, const TString& name);

EUdfValueType ParseValueType(TStringBuf type);

const char* ValueTypeToString(EUdfValueType type);

TVector<TWasmUdfDescriptor> ParseFunctionDescriptors(const TString& content);

TString DescriptorPathToModulePath(const TString& descriptorPath);

TString FindOptionalSdkPath(const TString& directory);

void AddModuleFromFile(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    const TString& path);

std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> CreateRegistryCompartment(
    const TString& directory,
    const TString& sdkPath);

TUnversionedValue MakeEmptyValue();

void StoreValue(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    uintptr_t offset,
    const TUnversionedValue& value);

struct TPreparedUdfArg
{
    NYdb::NWasm::TCopyGuard ValueGuard;
    NYdb::NWasm::TCopyGuard StringGuard;
    uintptr_t Offset = 0;
};

class TCurrentCompartmentGuard
{
public:
    explicit TCurrentCompartmentGuard(NYdb::NWasm::IWebAssemblyCompartment* compartment);
    ~TCurrentCompartmentGuard();

    TCurrentCompartmentGuard(const TCurrentCompartmentGuard&) = delete;
    TCurrentCompartmentGuard& operator=(const TCurrentCompartmentGuard&) = delete;

private:
    NYdb::NWasm::IWebAssemblyCompartment* Previous_;
};

//! Invoke a wasm UDF export with the YT calling convention:
//! signature: void(uintptr_t context, uintptr_t result, uintptr_t arg1, ...).
//! All arguments are passed as 64-bit values; arity is unrestricted.
void InvokeUdfExport(
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    const TString& functionName,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args);

} // namespace NWasm::NYQL
