#include "wasm_state.hpp"

#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NWasm::NYQL {

using namespace NYdb::NWasm;
using namespace WAVM;
using namespace WAVM::IR;

namespace {

TString ReadFileContent(const TString& path)
{
    TFileInput input(path);
    return input.ReadAll();
}

EWasmValueType FromValueType(ValueType type)
{
    switch (type) {
        case ValueType::i32:
            return EWasmValueType::I32;
        case ValueType::i64:
            return EWasmValueType::I64;
        case ValueType::f32:
            return EWasmValueType::F32;
        case ValueType::f64:
            return EWasmValueType::F64;
        default:
            return EWasmValueType::Unsupported;
    }
}

Module ParseWastModule(TStringBuf wast)
{
    Module module;
    module.featureSpec.memory64 = true;
    module.featureSpec.exceptionHandling = true;

    std::vector<WAST::Error> errors;
    const bool succeeded = WAST::parseModule(
        wast.data(),
        wast.size() + 1,
        module,
        errors);
    if (!succeeded) {
        TStringBuilder message;
        for (const auto& error : errors) {
            if (!message.empty()) {
                message << "; ";
            }
            message << error.message;
        }
        ythrow yexception() << "Failed to parse WAST module: " << message;
    }
    return module;
}

THashMap<TString, TWasmExportInfo> ExtractFunctionExports(const Module& module)
{
    THashMap<TString, TWasmExportInfo> exports;
    for (const auto& exportItem : module.exports) {
        if (exportItem.kind != ExternKind::function) {
            continue;
        }

        const auto& indexedType = module.functions.getType(exportItem.index);
        const auto& functionType = module.types[indexedType.index];

        TWasmExportInfo info;
        info.Name = exportItem.name;
        info.RuntimeTypeEncoding = functionType.getEncoding().impl;

        for (const auto paramType : functionType.params()) {
            info.Signature.Params.push_back(FromValueType(paramType));
        }

        const auto results = functionType.results();
        if (results.size() == 0) {
            info.Signature.Result = EWasmValueType::Void;
        } else if (results.size() == 1) {
            info.Signature.Result = FromValueType(results[0]);
        } else {
            info.Signature.Result = EWasmValueType::Unsupported;
        }

        info.Signature.Supported = IsSupportedScalarSignature(info.Signature);
        exports.emplace(info.Name, std::move(info));
    }
    return exports;
}

THashMap<TString, TWasmExportInfo> ExtractBinaryExports(TStringBuf bytecode)
{
    auto featureSpec = FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.exceptionHandling = true;

    auto loadError = WASM::LoadError();
    Runtime::ModuleRef wasmModule;
    const bool succeeded = Runtime::loadBinaryModule(
        std::bit_cast<const U8*>(bytecode.data()),
        bytecode.size(),
        wasmModule,
        featureSpec,
        &loadError);
    if (!succeeded) {
        ythrow yexception() << "Failed to load wasm binary module: " << loadError.message;
    }

    const auto& irModule = Runtime::getModuleIR(wasmModule);
    return ExtractFunctionExports(irModule);
}

THashMap<TString, TWasmExportInfo> ExtractExportsFromPath(
    const TString& path,
    const TString& bytecode)
{
    if (path.EndsWith(".wat") || path.EndsWith(".wast")) {
        return ExtractFunctionExports(ParseWastModule(bytecode));
    }
    return ExtractBinaryExports(bytecode);
}

} // namespace

THashMap<TString, TWasmExportInfo> ExtractWasmExportsFromPath(
    const TString& path,
    const TString& bytecode)
{
    return ExtractExportsFromPath(path, bytecode);
}

void WasmError(const std::exception& ex, TStringRef name, const IValueBuilder* valueBuilder)
{
    const auto pos = (!valueBuilder || !valueBuilder->CalleePosition())
        ? TSourcePosition{}
        : *valueBuilder->CalleePosition();
    const auto msg = TStringBuilder() << pos << "Wasm." << name << "(); ex: " << ex.what();
    UdfTerminate(msg.c_str());
}

TWasmRuntimeStatePtr LoadWasmModule(const TString& path)
{
    const auto bytecode = ReadFileContent(path);
    if (bytecode.empty()) {
        ythrow yexception() << "Wasm module file is empty: " << path;
    }

    auto state = std::make_shared<TWasmRuntimeState>();
    state->Exports = ExtractExportsFromPath(path, bytecode);
    state->Compartment = CreateMinimalRuntimeImage();
    if (path.EndsWith(".wat") || path.EndsWith(".wast")) {
        state->Compartment->AddModule(bytecode, path);
    } else {
        state->Compartment->AddModule(NYT::TRef::FromString(bytecode), path);
    }
    return state;
}

const TWasmExportInfo& GetWasmExport(
    const TWasmRuntimeStatePtr& state,
    const TString& functionName)
{
    Y_ENSURE(state);
    if (const auto* exportInfo = state->Exports.FindPtr(functionName)) {
        return *exportInfo;
    }
    ythrow yexception() << "Unknown wasm export: " << functionName;
}

TVector<TWasmExportInfo> ListWasmExports(const TWasmRuntimeStatePtr& state)
{
    Y_ENSURE(state);
    TVector<TWasmExportInfo> exports;
    exports.reserve(state->Exports.size());
    for (const auto& item : state->Exports) {
        exports.push_back(item.second);
    }
    Sort(exports.begin(), exports.end(), [](const auto& left, const auto& right) {
        return left.Name < right.Name;
    });
    return exports;
}

} // namespace NWasm::NYQL
