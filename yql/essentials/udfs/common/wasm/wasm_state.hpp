#pragma once

#include "wasm_signature.hpp"

#include <yql/essentials/public/udf/udf_helpers.h>

#include <ydb/library/wasm/api/compartment.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

struct TWasmRuntimeState
{
    std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> Compartment;
    THashMap<TString, TWasmExportInfo> Exports;
};

using TWasmRuntimeStatePtr = std::shared_ptr<TWasmRuntimeState>;

TWasmRuntimeStatePtr LoadWasmModule(const TString& path);

const TWasmExportInfo& GetWasmExport(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName);

TVector<TWasmExportInfo> ListWasmExports(const TWasmRuntimeStatePtr& state);

//! Parse a wasm/wat/wast/so file bytecode and return the set of exported functions and their signatures.
//! The decision between text (wat/wast) and binary parsing is made by the path extension.
THashMap<TString, TWasmExportInfo> ExtractWasmExportsFromPath(
    const TString& path,
    const TString& bytecode);

void WasmError(const std::exception& ex, TStringRef name, const IValueBuilder* valueBuilder);

} // namespace NWasm::NYQL
