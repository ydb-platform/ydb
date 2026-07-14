#pragma once

#include "wasm_state.hpp"

#include <yql/essentials/public/udf/udf_helpers.h>

#include <ydb/library/wasm/api/data_transfer.h>

#include <vector>

namespace NWasm::NYQL {

struct TWasmInvokeResult
{
    bool HasValue = false;
    TUnboxedValuePod Value;
    bool IsInt = false;
};

struct TWasmStringInvokeResult
{
    bool HasValue = false;
    TString Value;
};

struct TWasmPreparedArgs
{
    TVector<TUnboxedValuePod> Values;
    std::vector<NYdb::NWasm::TCopyGuard> MemoryGuards;
};

TWasmInvokeResult InvokeWasmFunction(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName,
    const TVector<TUnboxedValuePod>& args);

TWasmPreparedArgs ReadWasmArgsList(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argsValue,
    const TWasmFunctionSignature& signature);

TWasmPreparedArgs ReadWasmInt64ArgsList(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argsValue,
    const TWasmFunctionSignature& signature);

TWasmPreparedArgs ReadWasmStringArg(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argValue,
    const TWasmFunctionSignature& signature);

TWasmPreparedArgs ReadWasmJsonArgs(
    const TWasmRuntimeStatePtr& state,
    TStringBuf argsJson,
    const TWasmFunctionSignature& signature);

TWasmStringInvokeResult InvokeWasmStringFunction(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName,
    const TVector<TUnboxedValuePod>& args);

} // namespace NWasm::NYQL
