#include "registry_helpers.h"
#include "call_stack.h"
#include "compile.h"

#include <ydb/library/wasm/api/bytecode.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <bit>
#include <limits>

namespace {

// Bump allocator used when a UDF has empty required_libraries.
// CreateEmptyImage alone has host intrinsics but no RuntimeLibraryInstance_
// with wasm malloc/free; compartment->AllocateBytes needs those exports.
//
// Heap starts above a reserved low region so UDF data segments (e.g. at 1024)
// are not clobbered by the first malloc used for argument/result marshalling.
//
// malloc goes through "sbrk" rather than bumping the heap global directly,
// because the host moves that same break past regions it pins into linear
// memory (see IWebAssemblyCompartment::ReserveGuestHeapBelow). A stub without
// "sbrk" would let the resident cache and malloc hand out the same bytes.
//
// That fence leaves the break at the top of linear memory, so "sbrk" has to
// grow memory itself before handing anything out -- a pure pointer bump would
// return an offset the guest cannot store to. Real libc sbrk does the same.
constexpr TStringBuf DefaultRegistrySdkWast = R"WAST(
(module
    (import "env" "memory" (memory i64 8 2097152))
    (global $heap (mut i64) (i64.const 65536))
    (func $sbrk (param $n i64) (result i64)
        (local $p i64)
        (local $break i64)
        (local $pages i64)
        (local.set $p (global.get $heap))
        (local.set $break
            (i64.and
                (i64.add (i64.add (local.get $p) (local.get $n)) (i64.const 7))
                (i64.const -8)))
        (local.set $pages
            (i64.sub
                (i64.shr_u
                    (i64.add (local.get $break) (i64.const 65535))
                    (i64.const 16))
                (memory.size)))
        (if (i64.gt_s (local.get $pages) (i64.const 0))
            (then
                (if (i64.eq (memory.grow (local.get $pages)) (i64.const -1))
                    (then (return (i64.const -1))))))
        (global.set $heap (local.get $break))
        (local.get $p)
    )
    (func $malloc (param $n i64) (result i64)
        (call $sbrk (local.get $n))
    )
    (func $free (param $p i64))
    (export "sbrk" (func $sbrk))
    (export "malloc" (func $malloc))
    (export "free" (func $free))
)
)WAST";

NYdb::NWasm::TModuleBytecode MakeDefaultRegistrySdkBytecode() {
    using namespace NKikimr::NUdfStore::NWasm;
    using namespace NYdb::NWasm;
    const auto objectCode = CompileModuleObjectCode(
        DefaultRegistrySdkWast,
        EBytecodeFormat::HumanReadable);
    return MakeModuleBytecode(
        DefaultRegistrySdkWast,
        objectCode,
        EBytecodeFormat::HumanReadable);
}

} // namespace

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
    if (type == "int32") {
        return EUdfValueType::Int32;
    }
    if (type == "uint32") {
        return EUdfValueType::Uint32;
    }
    if (type == "float") {
        return EUdfValueType::Float;
    }
    if (type == "utf8") {
        return EUdfValueType::Utf8;
    }
    if (type == "date") {
        return EUdfValueType::Date;
    }
    if (type == "datetime") {
        return EUdfValueType::Datetime;
    }
    if (type == "timestamp") {
        return EUdfValueType::Timestamp;
    }
    if (type == "decimal") {
        return EUdfValueType::Decimal;
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
        case EUdfValueType::Int32:
            return "int32";
        case EUdfValueType::Uint32:
            return "uint32";
        case EUdfValueType::Float:
            return "float";
        case EUdfValueType::Utf8:
            return "utf8";
        case EUdfValueType::Date:
            return "date";
        case EUdfValueType::Datetime:
            return "datetime";
        case EUdfValueType::Timestamp:
            return "timestamp";
        case EUdfValueType::Decimal:
            return "decimal";
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
    // CreateImageFromSdk clones from a process-wide cache (first call is slow,
    // later Acquires are cheap). MinimalRuntime must not be used — it lacks
    // AllocateBytes / ThrowException.
    //
    // Empty required_libraries: install a default bump-allocator as "env"
    // (CreateEmptyImage alone has host intrinsics but no RuntimeLibraryInstance_
    // with wasm malloc/free that compartment->AllocateBytes needs).
    const TModuleBytecode* sdkBytecode = nullptr;
    static const auto defaultSdk = MakeDefaultRegistrySdkBytecode();
    if (libraries.empty()) {
        sdkBytecode = &defaultSdk;
    } else {
        for (const auto& library : libraries) {
            if (!library.Bytecode.ObjectCode) {
                ythrow yexception()
                    << "Precompiled object code is required for library '" << library.Name << "'";
            }
        }
        sdkBytecode = &libraries.front().Bytecode;
    }

    auto compartment = CreateImageFromSdk(*sdkBytecode);
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

ui32 CheckedAbiLength(size_t size, TStringBuf what) {
    if (size > std::numeric_limits<ui32>::max()) {
        ythrow yexception()
            << what << " length " << size
            << " exceeds ABI ui32 limit (" << std::numeric_limits<ui32>::max() << ")";
    }
    return static_cast<ui32>(size);
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
        // Type/args from WAVM, but only user wasm frames in the stack (like ThrowException).
        std::string message = WAVM::Runtime::describeException(exception);
        TString stack;
        try {
            stack = FormatUserWasmCallStack(WAVM::Runtime::getExceptionCallStack(exception));
        } catch (const std::exception& ex) {
            stack = TStringBuilder() << "<wasm call stack unavailable: " << ex.what() << ">\n";
        } catch (...) {
            stack = "<wasm call stack unavailable>\n";
        }
        WAVM::Runtime::destroyException(exception);

        const auto stackPos = message.find("\nCall stack:");
        if (stackPos != std::string::npos) {
            message.resize(stackPos);
        }

        // Plain throw: do not prefix with registry_helpers.cpp:line for users.
        throw yexception()
            << "WAVM runtime exception while calling \""
            << functionNameForErrors << "\": " << message
            << "\n\n" << stack;
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

namespace {

void CollectFunctionExports(
    const WAVM::IR::Module& module,
    THashMap<TString, TWasmExportSignature>& exports)
{
    for (const auto& exportItem : module.exports) {
        if (exportItem.kind != WAVM::IR::ExternKind::function) {
            continue;
        }
        // Function exports index the joint import+definition space; the type
        // they name is an index into the module's type section.
        const auto& functionType = module.types[module.functions.getType(exportItem.index).index];
        exports[TString(exportItem.name)] = TWasmExportSignature{
            .ParamCount = functionType.params().size(),
            .ResultCount = functionType.results().size(),
        };
    }
}

} // namespace

THashMap<TString, TWasmExportSignature> CollectWasmExports(
    TStringBuf bytes,
    EBytecodeFormat format)
{
    THashMap<TString, TWasmExportSignature> exports;

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
        CollectFunctionExports(module, exports);
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
    CollectFunctionExports(Runtime::getModuleIR(wasmModule), exports);
    return exports;
}

} // namespace NKikimr::NUdfStore::NWasm
