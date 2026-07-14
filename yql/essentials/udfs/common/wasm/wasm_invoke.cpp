#include "wasm_invoke.hpp"

#include "wasm_signature.hpp"

#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/yt/memory/range.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;
using namespace NYT;
using namespace NYdb::NWasm;

namespace {

constexpr ui32 Int64VariantIndex = 0;
constexpr ui32 DoubleVariantIndex = 1;
constexpr ui32 StringVariantIndex = 2;
constexpr ui32 Int64ListVariantIndex = 3;

class TCurrentCompartmentGuard
{
public:
    explicit TCurrentCompartmentGuard(IWebAssemblyCompartment* compartment)
    {
        SetCurrentCompartment(compartment);
    }

    ~TCurrentCompartmentGuard()
    {
        SetCurrentCompartment(nullptr);
    }

    TCurrentCompartmentGuard(const TCurrentCompartmentGuard&) = delete;
    TCurrentCompartmentGuard& operator=(const TCurrentCompartmentGuard&) = delete;
};

bool ExpectsIntegerArg(EWasmValueType type)
{
    return type == EWasmValueType::I32 || type == EWasmValueType::I64;
}

bool ExpectsFloatArg(EWasmValueType type)
{
    return type == EWasmValueType::F32 || type == EWasmValueType::F64;
}

TWavmPodValue ToWavmArgument(EWasmValueType type, const TUnboxedValuePod& value)
{
    TWavmPodValue arg;
    arg.Data = 0;
    switch (type) {
        case EWasmValueType::I32:
            arg.Data = static_cast<ui64>(static_cast<ui32>(value.Get<i64>()));
            return arg;
        case EWasmValueType::I64:
            arg.Data = static_cast<ui64>(value.Get<i64>());
            return arg;
        case EWasmValueType::F32: {
            const auto f32 = static_cast<float>(value.Get<double>());
            arg.Data = static_cast<ui64>(std::bit_cast<ui32>(f32));
            return arg;
        }
        case EWasmValueType::F64:
            arg.Data = std::bit_cast<ui64>(value.Get<double>());
            return arg;
        default:
            ythrow yexception() << "Unsupported wasm argument type: "
                << WasmValueTypeToString(type);
    }
}

TWasmInvokeResult FromWavmResult(EWasmValueType type, const TWavmPodValue& result)
{
    TWasmInvokeResult invokeResult;
    switch (type) {
        case EWasmValueType::Void:
            invokeResult.HasValue = false;
            return invokeResult;
        case EWasmValueType::I32:
        case EWasmValueType::I64:
            invokeResult.HasValue = true;
            invokeResult.IsInt = true;
            invokeResult.Value = TUnboxedValuePod(static_cast<i64>(result.Data));
            return invokeResult;
        case EWasmValueType::F32: {
            const float f32 = std::bit_cast<float>(static_cast<ui32>(result.Data));
            invokeResult.HasValue = true;
            invokeResult.IsInt = false;
            invokeResult.Value = TUnboxedValuePod(static_cast<double>(f32));
            return invokeResult;
        }
        case EWasmValueType::F64:
            invokeResult.HasValue = true;
            invokeResult.IsInt = false;
            invokeResult.Value = TUnboxedValuePod(std::bit_cast<double>(result.Data));
            return invokeResult;
        default:
            ythrow yexception() << "Unsupported wasm result type: "
                << WasmValueTypeToString(type);
    }
}

TUnboxedValuePod ReadScalarArg(
    const TUnboxedValue& value,
    EWasmValueType expectedType,
    size_t argIndex)
{
    const auto variantIndex = value.GetVariantIndex();
    const auto item = value.GetVariantItem();

    if (ExpectsIntegerArg(expectedType)) {
        if (variantIndex != Int64VariantIndex) {
            ythrow yexception()
                << "Wasm argument at index " << argIndex
                << " expects Variant<Int64, Double, String, List<Int64>> with Int64 branch (index "
                << Int64VariantIndex << "), got variant index " << variantIndex
                << " for wasm type " << WasmValueTypeToString(expectedType);
        }
        return item;
    }

    if (ExpectsFloatArg(expectedType)) {
        if (variantIndex != DoubleVariantIndex) {
            ythrow yexception()
                << "Wasm argument at index " << argIndex
                << " expects Variant<Int64, Double, String, List<Int64>> with Double branch (index "
                << DoubleVariantIndex << "), got variant index " << variantIndex
                << " for wasm type " << WasmValueTypeToString(expectedType);
        }
        return item;
    }

    ythrow yexception()
        << "Unsupported wasm argument type at index " << argIndex << ": "
        << WasmValueTypeToString(expectedType);
}

TUnboxedValuePod ReadInt64ScalarArg(
    i64 value,
    EWasmValueType expectedType,
    size_t argIndex)
{
    if (ExpectsIntegerArg(expectedType)) {
        return TUnboxedValuePod(value);
    }

    if (ExpectsFloatArg(expectedType)) {
        return TUnboxedValuePod(static_cast<double>(value));
    }

    ythrow yexception()
        << "Unsupported wasm argument type at index " << argIndex << ": "
        << WasmValueTypeToString(expectedType);
}

TUnboxedValuePod ReadJsonNumberArg(
    const NJson::TJsonValue& value,
    EWasmValueType expectedType,
    size_t argIndex)
{
    if (ExpectsIntegerArg(expectedType)) {
        if (!value.IsInteger()) {
            ythrow yexception()
                << "Wasm JSON argument at index " << argIndex
                << " expects integer for wasm type " << WasmValueTypeToString(expectedType);
        }
        return TUnboxedValuePod(static_cast<i64>(value.GetInteger()));
    }

    if (ExpectsFloatArg(expectedType)) {
        if (value.IsInteger()) {
            return TUnboxedValuePod(static_cast<double>(value.GetInteger()));
        }
        if (value.IsDouble()) {
            return TUnboxedValuePod(value.GetDouble());
        }
        ythrow yexception()
            << "Wasm JSON argument at index " << argIndex
            << " expects number for wasm type " << WasmValueTypeToString(expectedType);
    }

    ythrow yexception()
        << "Unsupported wasm argument type at index " << argIndex << ": "
        << WasmValueTypeToString(expectedType);
}

void EnsurePtrLenParams(
    const TWasmFunctionSignature& signature,
    size_t paramIndex,
    size_t argIndex,
    TStringBuf argKind)
{
    if (paramIndex + 1 >= signature.Params.size()
        || !ExpectsIntegerArg(signature.Params[paramIndex])
        || !ExpectsIntegerArg(signature.Params[paramIndex + 1]))
    {
        ythrow yexception()
            << "Wasm memory argument at index " << argIndex << " (" << argKind
            << ") expands to (ptr, len), but wasm signature does not have two integer parameters at position "
            << paramIndex;
    }
}

void AppendStringArg(
    TWasmPreparedArgs& prepared,
    IWebAssemblyCompartment* compartment,
    const TUnboxedValuePod& value)
{
    const TStringBuf string(value.AsStringRef());
    auto guard = CopyIntoCompartment(string, compartment);
    const auto offset = guard.GetCopiedOffset();
    prepared.MemoryGuards.push_back(std::move(guard));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(offset)));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(string.size())));
}

void AppendStringBufArg(
    TWasmPreparedArgs& prepared,
    IWebAssemblyCompartment* compartment,
    TStringBuf string)
{
    auto guard = CopyIntoCompartment(string, compartment);
    const auto offset = guard.GetCopiedOffset();
    prepared.MemoryGuards.push_back(std::move(guard));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(offset)));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(string.size())));
}

void AppendInt64ListArg(
    TWasmPreparedArgs& prepared,
    IWebAssemblyCompartment* compartment,
    const TUnboxedValuePod& value)
{
    std::vector<i64> data;
    data.reserve(value.GetListLength());

    auto iterator = value.GetListIterator();
    TUnboxedValue item;
    while (iterator.Next(item)) {
        data.push_back(item.Get<i64>());
    }

    auto guard = CopyIntoCompartment<const std::vector<i64>&>(data, compartment);
    const auto offset = guard.GetCopiedOffset();
    prepared.MemoryGuards.push_back(std::move(guard));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(offset)));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(data.size())));
}

void AppendJsonInt64ArrayArg(
    TWasmPreparedArgs& prepared,
    IWebAssemblyCompartment* compartment,
    const NJson::TJsonValue& value,
    size_t argIndex)
{
    if (!value.IsArray()) {
        ythrow yexception() << "Wasm JSON argument at index " << argIndex << " expects array";
    }

    std::vector<i64> data;
    const auto& array = value.GetArray();
    data.reserve(array.size());
    for (const auto& item : array) {
        if (!item.IsInteger()) {
            ythrow yexception()
                << "Wasm JSON memory argument at index " << argIndex
                << " expects array of integers";
        }
        data.push_back(static_cast<i64>(item.GetInteger()));
    }

    auto guard = CopyIntoCompartment<const std::vector<i64>&>(data, compartment);
    const auto offset = guard.GetCopiedOffset();
    prepared.MemoryGuards.push_back(std::move(guard));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(offset)));
    prepared.Values.push_back(TUnboxedValuePod(static_cast<i64>(data.size())));
}

void AppendJsonArg(
    TWasmPreparedArgs& prepared,
    IWebAssemblyCompartment* compartment,
    const TWasmFunctionSignature& signature,
    const NJson::TJsonValue& value,
    size_t argIndex,
    size_t& paramIndex)
{
    if (value.IsString()) {
        EnsurePtrLenParams(signature, paramIndex, argIndex, "String");
        AppendStringBufArg(prepared, compartment, value.GetString());
        paramIndex += 2;
        return;
    }

    if (value.IsArray()) {
        EnsurePtrLenParams(signature, paramIndex, argIndex, "List<Int64>");
        AppendJsonInt64ArrayArg(prepared, compartment, value, argIndex);
        paramIndex += 2;
        return;
    }

    if (paramIndex >= signature.Params.size()) {
        ythrow yexception()
            << "Wasm function got too many JSON arguments; extra argument at index " << argIndex;
    }
    prepared.Values.push_back(ReadJsonNumberArg(value, signature.Params[paramIndex], argIndex));
    ++paramIndex;
}

bool IsJsonInt64Array(const NJson::TJsonValue& value)
{
    if (!value.IsArray()) {
        return false;
    }
    for (const auto& item : value.GetArray()) {
        if (!item.IsInteger()) {
            return false;
        }
    }
    return true;
}

} // namespace

TWasmPreparedArgs ReadWasmArgsList(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argsValue,
    const TWasmFunctionSignature& signature)
{
    Y_ENSURE(state);
    Y_ENSURE(state->Compartment);

    TWasmPreparedArgs prepared;
    prepared.Values.reserve(signature.Params.size());

    auto iterator = argsValue.GetListIterator();
    size_t argIndex = 0;
    size_t paramIndex = 0;
    for (; argIndex < argsValue.GetListLength(); ++argIndex) {
        TUnboxedValue item;
        if (!iterator.Next(item)) {
            ythrow yexception() << "Failed to read wasm argument at index " << argIndex;
        }

        const auto variantIndex = item.GetVariantIndex();
        const auto variantItem = item.GetVariantItem();
        if (variantIndex == StringVariantIndex) {
            EnsurePtrLenParams(signature, paramIndex, argIndex, "String");
            AppendStringArg(prepared, state->Compartment.get(), variantItem);
            paramIndex += 2;
        } else if (variantIndex == Int64ListVariantIndex) {
            EnsurePtrLenParams(signature, paramIndex, argIndex, "List<Int64>");
            AppendInt64ListArg(prepared, state->Compartment.get(), variantItem);
            paramIndex += 2;
        } else {
            if (paramIndex >= signature.Params.size()) {
                ythrow yexception()
                    << "Wasm function got too many arguments; extra argument at index " << argIndex;
            }
            prepared.Values.push_back(ReadScalarArg(item, signature.Params[paramIndex], argIndex));
            ++paramIndex;
        }
    }

    if (paramIndex != signature.Params.size()) {
        ythrow yexception()
            << "Wasm function expects " << signature.Params.size()
            << " wasm parameter(s) after memory argument expansion, got " << paramIndex;
    }
    return prepared;
}

TWasmPreparedArgs ReadWasmInt64ArgsList(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argsValue,
    const TWasmFunctionSignature& signature)
{
    Y_ENSURE(state);
    Y_ENSURE(state->Compartment);

    if (argsValue.GetListLength() != signature.Params.size()) {
        TWasmPreparedArgs prepared;
        prepared.Values.reserve(2);
        EnsurePtrLenParams(signature, 0, 0, "List<Int64>");
        AppendInt64ListArg(prepared, state->Compartment.get(), argsValue);
        return prepared;
    }

    TWasmPreparedArgs prepared;
    prepared.Values.reserve(signature.Params.size());

    auto iterator = argsValue.GetListIterator();
    for (size_t argIndex = 0; argIndex < argsValue.GetListLength(); ++argIndex) {
        TUnboxedValue item;
        if (!iterator.Next(item)) {
            ythrow yexception() << "Failed to read wasm argument at index " << argIndex;
        }

        prepared.Values.push_back(
            ReadInt64ScalarArg(item.Get<i64>(), signature.Params[argIndex], argIndex));
    }

    return prepared;
}

TWasmPreparedArgs ReadWasmStringArg(
    const TWasmRuntimeStatePtr& state,
    const TUnboxedValuePod& argValue,
    const TWasmFunctionSignature& signature)
{
    Y_ENSURE(state);
    Y_ENSURE(state->Compartment);

    TWasmPreparedArgs prepared;
    prepared.Values.reserve(2);
    EnsurePtrLenParams(signature, 0, 0, "String");
    AppendStringArg(prepared, state->Compartment.get(), argValue);
    return prepared;
}

TWasmPreparedArgs ReadWasmJsonArgs(
    const TWasmRuntimeStatePtr& state,
    TStringBuf argsJson,
    const TWasmFunctionSignature& signature)
{
    Y_ENSURE(state);
    Y_ENSURE(state->Compartment);

    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(argsJson, &root)) {
        ythrow yexception() << "Failed to parse wasm JSON arguments: " << argsJson;
    }
    if (!root.IsArray()) {
        ythrow yexception() << "Wasm JSON arguments must be an array";
    }

    const auto& args = root.GetArray();
    TWasmPreparedArgs prepared;
    prepared.Values.reserve(signature.Params.size());

    if (args.size() != signature.Params.size() && IsJsonInt64Array(root)) {
        TWasmPreparedArgs memoryPrepared;
        memoryPrepared.Values.reserve(2);
        EnsurePtrLenParams(signature, 0, 0, "List<Int64>");
        AppendJsonInt64ArrayArg(memoryPrepared, state->Compartment.get(), root, 0);
        return memoryPrepared;
    }

    size_t paramIndex = 0;
    for (size_t argIndex = 0; argIndex < args.size(); ++argIndex) {
        AppendJsonArg(
            prepared,
            state->Compartment.get(),
            signature,
            args[argIndex],
            argIndex,
            paramIndex);
    }

    if (paramIndex != signature.Params.size()) {
        ythrow yexception()
            << "Wasm function expects " << signature.Params.size()
            << " wasm parameter(s) after JSON memory argument expansion, got " << paramIndex;
    }
    return prepared;
}

TWasmInvokeResult InvokeWasmFunction(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName,
    const TVector<TUnboxedValuePod>& args)
{
    const auto& exportInfo = GetWasmExport(state, functionName);
    if (!exportInfo.Signature.Supported) {
        ythrow yexception()
            << "Wasm function \"" << functionName << "\" has unsupported signature";
    }
    if (args.size() != exportInfo.Signature.Params.size()) {
        ythrow yexception()
            << "Wasm function \"" << functionName << "\" expects "
            << exportInfo.Signature.Params.size() << " argument(s), got " << args.size();
    }

    Y_ENSURE(state->Compartment);
    if (!state->Compartment->GetFunction(functionName)) {
        ythrow yexception() << "Unknown wasm export: " << functionName;
    }

    std::array<TWavmPodValue, 16> wavmArgsStorage;
    if (args.size() > wavmArgsStorage.size()) {
        ythrow yexception() << "Too many wasm arguments: " << args.size();
    }

    for (size_t i = 0; i < args.size(); ++i) {
        wavmArgsStorage[i] = ToWavmArgument(exportInfo.Signature.Params[i], args[i]);
    }

    TCurrentCompartmentGuard compartmentGuard(state->Compartment.get());
    try {
        const auto runtimeFunction = state->Compartment->GetFunction(functionName);
        const auto runtimeType = TWebAssemblyRuntimeType{
            std::bit_cast<void*>(exportInfo.RuntimeTypeEncoding)};

        if (exportInfo.Signature.Result == EWasmValueType::Void) {
            NYdb::NWasm::NDetail::WavmInvoke(
                state->Compartment.get(),
                runtimeType,
                runtimeFunction,
                nullptr,
                TRange(wavmArgsStorage.data(), args.size()));
            return {};
        }

        TWavmPodValue wavmResult;
        wavmResult.Data = 0;
        NYdb::NWasm::NDetail::WavmInvoke(
            state->Compartment.get(),
            runtimeType,
            runtimeFunction,
            &wavmResult,
            TRange(wavmArgsStorage.data(), args.size()));
        return FromWavmResult(exportInfo.Signature.Result, wavmResult);
    } catch (WAVM::Runtime::Exception* exception) {
        const auto message = WAVM::Runtime::describeException(exception);
        WAVM::Runtime::destroyException(exception);
        ythrow yexception() << "WAVM runtime exception: " << message;
    }
}

TWasmStringInvokeResult InvokeWasmStringFunction(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName,
    const TVector<TUnboxedValuePod>& args)
{
    const auto invokeResult = InvokeWasmFunction(state, functionName, args);
    if (!invokeResult.HasValue) {
        return {};
    }
    if (!invokeResult.IsInt) {
        ythrow yexception()
            << "Wasm function \"" << functionName
            << "\" must return an integer pointer for string result";
    }

    const auto offset = static_cast<uintptr_t>(invokeResult.Value.Get<i64>());
    const auto* hostString = PtrFromVM(
        state->Compartment.get(),
        std::bit_cast<const char*>(offset));

    TWasmStringInvokeResult stringResult;
    stringResult.HasValue = true;
    stringResult.Value = TString(TStringBuf(hostString));
    return stringResult;
}

} // namespace NWasm::NYQL
