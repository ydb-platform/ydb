#include "registry_helpers.h"
#include "compile.h"

#include <ydb/library/wasm/api/bytecode.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/yexception.h>
#include <util/string/printf.h>

#include <bit>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYdb::NWasm;

EUdfValueType ParseValueType(TStringBuf type) {
    if (type == "int64") {
        return EUdfValueType::Int64;
    }
    if (type == "uint64") {
        return EUdfValueType::Uint64;
    }
    if (type == "double") {
        return EUdfValueType::Double;
    }
    if (type == "boolean" || type == "bool") {
        return EUdfValueType::Boolean;
    }
    if (type == "string") {
        return EUdfValueType::String;
    }
    if (type == "null") {
        return EUdfValueType::Null;
    }
    ythrow yexception() << "Unsupported wasm UDF descriptor type: " << type;
}

const char* ValueTypeToString(EUdfValueType type) {
    switch (type) {
        case EUdfValueType::Null:
            return "null";
        case EUdfValueType::Int64:
            return "int64";
        case EUdfValueType::Uint64:
            return "uint64";
        case EUdfValueType::Double:
            return "double";
        case EUdfValueType::Boolean:
            return "boolean";
        case EUdfValueType::String:
            return "string";
    }
    return "unknown";
}

NYdb::NWasm::TModuleBytecode MakeModuleBytecode(
    TStringBuf wasmData,
    TStringBuf objectCode,
    NYdb::NWasm::EBytecodeFormat format)
{
    TModuleBytecode bytecode;
    bytecode.Format = format;
    bytecode.Data = TSharedRef::FromString(TString(wasmData));
    if (!objectCode.empty()) {
        bytecode.ObjectCode = TSharedRef::FromString(TString(objectCode));
    }
    return bytecode;
}

void AddPrecompiledModule(
    IWebAssemblyCompartment* compartment,
    const TModuleBytecode& bytecode,
    TStringBuf name)
{
    if (!bytecode.ObjectCode) {
        ythrow yexception() << "Precompiled module object code is required for '" << name << "'";
    }
    compartment->AddPrecompiledModule(bytecode, name);
}

std::unique_ptr<IWebAssemblyCompartment> CreateRegistryCompartment(
    const TVector<TNamedModuleBytecode>& libraries)
{
    // Without user libraries, use Empty (standard host intrinsics as "env").
    // MinimalRuntime uses the empty intrinsic module and does not export
    // AllocateBytes / ThrowException that WASM UDFs import from "env".
    if (libraries.empty()) {
        return CreateEmptyImage();
    }

    // User-provided runtime libraries (e.g. "sdk") must be installed as "env"
    // via AddSdk / CreateImageFromSdk. Loading them with CreateMinimalRuntimeImage
    // + name "sdk" leaves the minimal runtime in front of the linker and causes
    // import type mismatches.
    for (const auto& library : libraries) {
        if (!library.Bytecode.ObjectCode) {
            ythrow yexception()
                << "Precompiled object code is required for library '" << library.Name << "'";
        }
    }

    auto compartment = CreateImageFromSdk(libraries.front().Bytecode);
    for (size_t i = 1; i < libraries.size(); ++i) {
        AddPrecompiledModule(
            compartment.get(),
            libraries[i].Bytecode,
            libraries[i].Name);
    }
    return compartment;
}

TUnversionedValue MakeEmptyValue() {
    TUnversionedValue value{};
    value.Type = EAbiValueType::Null;
    value.Flags = EAbiValueFlags::None;
    return value;
}

void StoreValue(IWebAssemblyCompartment* compartment, uintptr_t offset, const TUnversionedValue& value) {
    auto* destination = PtrFromVM(compartment, std::bit_cast<TUnversionedValue*>(offset));
    *destination = value;
}

TCurrentCompartmentGuard::TCurrentCompartmentGuard(IWebAssemblyCompartment* compartment)
    : Previous_(GetCurrentCompartment())
{
    SetCurrentCompartment(compartment);
}

TCurrentCompartmentGuard::~TCurrentCompartmentGuard() {
    SetCurrentCompartment(Previous_);
}

void InvokeUdfExport(
    IWebAssemblyCompartment* compartment,
    void* runtimeFunction,
    const TString& functionNameForErrors,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args)
{
    if (runtimeFunction == nullptr) {
        ythrow yexception() << "Unknown wasm export: " << functionNameForErrors;
    }

    constexpr size_t kMaxArgs = 32;
    const size_t totalArgs = 2 + args.size();
    if (totalArgs > kMaxArgs) {
        ythrow yexception() << "Too many wasm UDF arguments: " << args.size();
    }

    std::array<EWebAssemblyValueType, kMaxArgs> argumentTypes;
    for (size_t i = 0; i < totalArgs; ++i) {
        argumentTypes[i] = EWebAssemblyValueType::UintPtr;
    }
    const auto runtimeType = GetTypeId(
        /*intrinsic*/ false,
        EWebAssemblyValueType::Void,
        TRange(argumentTypes.data(), totalArgs));

    std::array<TWavmPodValue, kMaxArgs> wavmArgs{};
    wavmArgs[0].Data = context;
    wavmArgs[1].Data = result;
    for (size_t i = 0; i < args.size(); ++i) {
        wavmArgs[2 + i].Data = args[i];
    }

    try {
        NYdb::NWasm::NDetail::WavmInvoke(
            compartment,
            runtimeType,
            runtimeFunction,
            /*result*/ nullptr,
            TRange(wavmArgs.data(), totalArgs));
    } catch (WAVM::Runtime::Exception* exception) {
        const auto message = WAVM::Runtime::describeException(exception);
        WAVM::Runtime::destroyException(exception);
        ythrow yexception() << "WAVM runtime exception while calling \""
            << functionNameForErrors << "\": " << message;
    }
}

void InvokeUdfExport(
    IWebAssemblyCompartment* compartment,
    const TString& functionName,
    uintptr_t context,
    uintptr_t result,
    const TVector<uintptr_t>& args)
{
    InvokeUdfExport(
        compartment,
        compartment->GetFunction(std::string(functionName)),
        functionName,
        context,
        result,
        args);
}

THashSet<TString> CollectWasmExports(TStringBuf bytes, EBytecodeFormat format) {
    THashSet<TString> exports;

    using namespace WAVM;
    using namespace WAVM::IR;

    if (format == EBytecodeFormat::HumanReadable) {
        Module module;
        module.featureSpec.memory64 = true;
        module.featureSpec.table64 = true;
        module.featureSpec.exceptionHandling = true;
        std::vector<WAST::Error> errors;
        if (!WAST::parseModule(bytes.data(), bytes.size() + 1, module, errors)) {
            ythrow yexception() << "Failed to parse WAST module";
        }
        for (const auto& exportItem : module.exports) {
            if (exportItem.kind == ExternKind::function) {
                exports.insert(TString(exportItem.name));
            }
        }
        return exports;
    }

    auto featureSpec = FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.table64 = true;
    featureSpec.exceptionHandling = true;
    auto loadError = WASM::LoadError();
    Runtime::ModuleRef wasmModule;
    if (!Runtime::loadBinaryModule(
            std::bit_cast<const U8*>(bytes.data()),
            bytes.size(),
            wasmModule,
            featureSpec,
            &loadError))
    {
        ythrow yexception() << "Failed to load wasm binary module: " << loadError.message;
    }
    const auto& irModule = Runtime::getModuleIR(wasmModule);
    for (const auto& exportItem : irModule.exports) {
        if (exportItem.kind == ExternKind::function) {
            exports.insert(TString(exportItem.name));
        }
    }
    return exports;
}

} // namespace NKikimr::NUdfStore::NWasm
