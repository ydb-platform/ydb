#include "wavm_private_imports.h"
#include "intrinsics.h"
#include "system_libraries.h"

#include <ydb/library/wasm/api/compartment.h>

#include <ydb/library/wasm/engine/builtins.h>

#include <library/cpp/resource/resource.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>
#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/tls.h>
#include <library/cpp/yt/string/format.h>

#include <contrib/restricted/wavm_llvm16/Lib/Runtime/RuntimePrivate.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/system/mutex.h>
#include <util/system/type_name.h>

using NYT::FormatValue;
using NYT::MakeFormattableView;
using NYT::New;
using NYT::TCompactVector;
using NYT::TStringBuilderBase;

namespace NYdb::NWasm {

using namespace WAVM;

////////////////////////////////////////////////////////////////////////////////

/*
 *    Memory Layout:
 *    +----------------------------------+          +---------------------+-----+
 *    | system libs | <- stack | heap -> |          | Global Offset Table | ... |
 *    +----------------------------------+          +---------------------+-----+
 *    0             512k       576k      128G       0             min: 2048
 *
 */

static constexpr I64 PageSize = 64_KB;
static constexpr I64 Padding = 64;

static constexpr I64 SystemLibsSize = 30_MB;
static constexpr I64 StackMaxSize = 64_KB;
static constexpr I64 MaxMemorySize  = 128_GB;

static constexpr I64 MemoryBase = 0;
static constexpr I64 SystemLibsLow = MemoryBase;
static constexpr I64 SystemLibsHigh = SystemLibsLow + SystemLibsSize;
static constexpr I64 StackLow = SystemLibsHigh + Padding;
static constexpr I64 StackHigh = StackLow + StackMaxSize;
static constexpr I64 HeapBase = StackHigh + Padding;
static constexpr I64 MinMemorySize = HeapBase + 512_KB;

static constexpr I64 StackPointer = StackHigh;

static constexpr I64 TableBase = 0;
static constexpr I32 TableBase32 = 0;

static constexpr U64 MinGlobalOffsetTableSize = 2048;

struct TMemoryLayoutData
{
    Runtime::GCPointer<Runtime::Memory> LinearMemory;
    Runtime::GCPointer<Runtime::Table> GlobalOffsetTable;
    Runtime::GCPointer<Runtime::Global> StackPointer;
    Runtime::GCPointer<Runtime::Global> HeapBase;
    Runtime::GCPointer<Runtime::Global> MemoryBase;
    Runtime::GCPointer<Runtime::Global> TableBase;
    Runtime::GCPointer<Runtime::Global> TableBase32;
    Runtime::GCPointer<Runtime::Global> StackLow;
    Runtime::GCPointer<Runtime::Global> StackHigh;

    std::vector<Uptr> MemoryBases = { 0ull };
    std::vector<Uptr> TableBases = { 0ull };

    TMemoryLayoutData BuildMemoryLayoutData(Runtime::Compartment* compartment);
    static void Clear(TMemoryLayoutData* data);
};

TMemoryLayoutData BuildMemoryLayoutData(Runtime::Compartment* compartment)
{
    static const auto mutableI64Global = IR::GlobalType{IR::ValueType::i64, true};
    static const auto immutableI64Global = IR::GlobalType{IR::ValueType::i64, false};
    static const auto immutableI32Global = IR::GlobalType{IR::ValueType::i32, false};

    auto data = TMemoryLayoutData{
        .LinearMemory = Runtime::createMemory(
            compartment,
            IR::MemoryType{
                /*isShared*/ false,
                /*indexType*/ IR::IndexType::i64,
                /*size*/ IR::SizeConstraints{MinMemorySize / PageSize, MaxMemorySize / PageSize}
            },
            "__linear_memory"),
        .GlobalOffsetTable = Runtime::createTable(
            compartment,
            IR::TableType{
                /*elementType*/ IR::ReferenceType::funcref,
                /*isShared*/ false,
                /*indexType*/ IR::IndexType::i32,
                /*size*/ IR::SizeConstraints{MinGlobalOffsetTableSize, std::numeric_limits<ui64>::max()},
            },
            nullptr,
            "__global_offset_table"),
        .StackPointer = Runtime::createGlobal(compartment, mutableI64Global, "__stack_pointer"),
        .HeapBase = Runtime::createGlobal(compartment, mutableI64Global, "__heap_base"),
        .MemoryBase = Runtime::createGlobal(compartment, immutableI64Global, "__memory_base"),
        .TableBase = Runtime::createGlobal(compartment, immutableI64Global, "__table_base"),
        .TableBase32 = Runtime::createGlobal(compartment, immutableI32Global, "__table_base32"),
        .StackLow = Runtime::createGlobal(compartment, mutableI64Global, "__stack_low"),
        .StackHigh = Runtime::createGlobal(compartment, mutableI64Global, "__stack_high"),
    };

    initializeGlobal(data.StackPointer, IR::Value{StackPointer});
    initializeGlobal(data.HeapBase, IR::Value{HeapBase});
    initializeGlobal(data.MemoryBase, IR::Value{MemoryBase});
    initializeGlobal(data.TableBase, IR::Value{TableBase});
    initializeGlobal(data.TableBase32, IR::Value{TableBase32});
    initializeGlobal(data.StackLow, IR::Value{StackLow});
    initializeGlobal(data.StackHigh, IR::Value{StackHigh});

    return data;
}

void TMemoryLayoutData::Clear(TMemoryLayoutData* data)
{
    data->LinearMemory = nullptr;
    data->GlobalOffsetTable = nullptr;
    data->StackPointer = nullptr;
    data->HeapBase = nullptr;
    data->MemoryBase = nullptr;
    data->TableBase = nullptr;
    data->TableBase32 = nullptr;
    data->StackLow = nullptr;
    data->StackHigh = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

Runtime::ModuleRef LoadModuleFromBytecode(TRef bytecode)
{
    auto featureSpec = IR::FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.exceptionHandling = true;

    auto loadError = WASM::LoadError();
    auto wasmModule = Runtime::ModuleRef();

    bool succeeded = Runtime::loadBinaryModule(
        std::bit_cast<const U8*>(bytecode.Begin()),
        bytecode.size(),
        wasmModule,
        featureSpec,
        &loadError);

    if (!succeeded) {
        THROW_ERROR_EXCEPTION("Could not load binary module: %v", loadError.message);
    }

    return wasmModule;
}

IR::Module ParseWast(TStringBuf wast)
{
    auto irModule = IR::Module();
    irModule.featureSpec.memory64 = true;
    irModule.featureSpec.exceptionHandling = true;

    auto wastErrors = std::vector<WAST::Error>();

    bool succeeded = WAST::parseModule(
        wast.data(),
        wast.size() + 1, // String must be zero-terminated.
        irModule,
        wastErrors);

    if (!succeeded) {
        THROW_ERROR_EXCEPTION(
            "Incorrect Wast file: %v",
            MakeFormattableView(
                wastErrors,
                [] (TStringBuilderBase* builder, const auto& error) {
                    FormatValue(builder, error.message, "v");
                }));
    }

    return irModule;
}

////////////////////////////////////////////////////////////////////////////////

struct TNamedGlobalOffsetTableElements
{
    THashMap<std::string, int> Functions;
    THashMap<std::string, int> DataEntries;
};

Uptr GetActiveElemSegmentSize(const IR::ElemSegment& elemSegment)
{
    if (elemSegment.type != IR::ElemSegment::Type::active) {
        return 0;
    }

    switch (elemSegment.contents->encoding) {
        case IR::ElemSegment::Encoding::expr:
            return elemSegment.contents->elemExprs.size();
        case IR::ElemSegment::Encoding::index:
            return elemSegment.contents->elemIndices.size();
        default:
            return 0;
    }
}

void EnsureGlobalOffsetTableCapacity(Runtime::Table* globalOffsetTable, Uptr requiredSize)
{
    const Uptr currentSize = Runtime::getTableNumElements(globalOffsetTable);
    if (currentSize >= requiredSize) {
        return;
    }

    const auto growResult = Runtime::growTable(globalOffsetTable, requiredSize - currentSize, nullptr);
    THROW_ERROR_EXCEPTION_IF(
        growResult != Runtime::GrowResult::success,
        "WebAssembly grow GOT error: failed to grow global offset table to %v elements",
        requiredSize);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EKnownImage,
    (Empty)
    (MinimalRuntime)
    (Standard)
    (QueryLanguage)
);

////////////////////////////////////////////////////////////////////////////////

// NB: Since pointer to current compartment is stored inside of a thread local,
// calls to context-switching functions should be guarded via this function.
template <typename TFunction>
auto SaveAndRestoreCompartment(IWebAssemblyCompartment* compartment, const TFunction& function) -> decltype(function())
{
    auto* savedCompartment = GetCurrentCompartment();

    Y_DEFER {
        YT_VERIFY(GetCurrentCompartment() == compartment);
        SetCurrentCompartment(savedCompartment);
    };

    SetCurrentCompartment(compartment);

    return function();
}

////////////////////////////////////////////////////////////////////////////////

class TWebAssemblyCompartment
    : public IWebAssemblyCompartment
{
public:
    TWebAssemblyCompartment() = default;

    ~TWebAssemblyCompartment()
    {
        IntrinsicsInstance_ = nullptr;
        RuntimeLibraryInstance_ = nullptr;

        for (auto& instance : Instances_) {
            instance = nullptr;
        }

        ExceptionType_ = nullptr;

        Context_ = nullptr;
        TMemoryLayoutData::Clear(&MemoryLayoutData_);

        auto collected = Runtime::tryCollectCompartment(std::move(Compartment_));
        YT_ASSERT(collected);
    }

    void AddModule(TRef bytecode, TStringBuf name = "") override
    {
        auto wavmModule = LoadModuleFromBytecode(bytecode);
        const auto& irModule = Runtime::getModuleIR(wavmModule);
        auto linkResult = LinkModule(irModule);
        AddExportsToGlobalOffsetTable(irModule);
        InstantiateModule(wavmModule, linkResult, name);
    }

    void AddModule(TStringBuf wast, TStringBuf name = "") override
    {
        auto irModule = ParseWast(wast);
        auto wavmModule = Runtime::compileModule(irModule);
        auto linkResult = LinkModule(irModule);
        AddExportsToGlobalOffsetTable(irModule);
        InstantiateModule(wavmModule, linkResult, name);
    }

    void AddPrecompiledModule(const TModuleBytecode& bytecode, TStringBuf name = "") override
    {
        if (!bytecode.ObjectCode) {
            THROW_ERROR_EXCEPTION("Precompiled module object code is required");
        }

        switch (bytecode.Format) {
            case EBytecodeFormat::HumanReadable: {
                THROW_ERROR_EXCEPTION("Human-readable precompiled modules are not supported");
                break;
            }

            case EBytecodeFormat::Binary: {
                auto featureSpec = IR::FeatureSpec();
                featureSpec.memory64 = true;
                featureSpec.table64 = true;
                featureSpec.exceptionHandling = true;

                auto irModule = IR::Module(std::move(featureSpec));

                auto loadError = WASM::LoadError();
                bool succeeded = WASM::loadBinaryModule(
                    std::bit_cast<U8*>(bytecode.Data.begin()),
                    bytecode.Data.size(),
                    irModule,
                    &loadError);

                if (!succeeded) {
                    THROW_ERROR_EXCEPTION("Could not load WebAssembly module: %v", loadError.message);
                }

                auto objectCode = std::vector<U8>(bytecode.ObjectCode.size());
                ::memcpy(objectCode.data(), bytecode.ObjectCode.data(), bytecode.ObjectCode.size());
                auto wavmModule = std::make_shared<Runtime::Module>(std::move(irModule), std::move(objectCode));
                const auto& runtimeIR = Runtime::getModuleIR(wavmModule);
                auto linkResult = LinkModule(runtimeIR);
                AddExportsToGlobalOffsetTable(runtimeIR);
                InstantiateModule(wavmModule, linkResult, name);
                break;
            }
        }
    }

    void AddSdk(const TModuleBytecode& bytecode) override
    {
        YT_ASSERT(!RuntimeLibraryInstance_);
        YT_ASSERT(Compartment_->instances.size() == 1);

        switch (bytecode.Format) {
            case EBytecodeFormat::HumanReadable: {
                THROW_ERROR_EXCEPTION("Human-readable runtime library files are not supported");
                break;
            }

            case EBytecodeFormat::Binary: {
                if (bytecode.ObjectCode) {
                    AddPrecompiledModule(bytecode, "env");
                    RuntimeLibraryInstance_ = Instances_.back();
                } else {
                    auto featureSpec = IR::FeatureSpec();
                    featureSpec.memory64 = true;
                    featureSpec.table64 = true;
                    featureSpec.exceptionHandling = true;

                    auto irModule = IR::Module(std::move(featureSpec));

                    auto loadError = WASM::LoadError();
                    bool succeeded = WASM::loadBinaryModule(
                        std::bit_cast<U8*>(bytecode.Data.begin()),
                        bytecode.Data.size(),
                        irModule,
                        &loadError);

                    if (!succeeded) {
                        THROW_ERROR_EXCEPTION("Could not load WebAssembly runtime library: %v", loadError.message);
                    }

                    auto linkResult = LinkModule(irModule);
                    auto sdkModule = Runtime::compileModule(irModule);
                    const auto& runtimeIR = Runtime::getModuleIR(sdkModule);
                    AddExportsToGlobalOffsetTable(runtimeIR);
                    InstantiateModule(sdkModule, linkResult, "env");
                    RuntimeLibraryInstance_ = Instances_.back();
                }

                break;
            }
        }
    }

    // Strip erases the linking metadata. This can speed up the clone operation.
    // After stripping, the compartment can execute loaded functions, but further linking is no longer possible.
    void Strip() override
    {
        YT_ASSERT(!Stripped_);
        Stripped_ = true;

        static const std::vector<std::string> shouldSaveExports{
            "malloc",
            "free",
            "EvaluateExpression",
            "EvaluateQuery",
            "init",
            "update",
            "merge",
            "finalize",
        };

        for (auto& instance : Instances_) {
            auto strippedExportMap = HashMap<std::string, Runtime::Object*>();
            for (const auto& item : shouldSaveExports) {
                auto it = instance->exportMap.get(item);
                if (it) {
                    strippedExportMap.add(item, *it);
                }
            }
            instance->exportMap = std::move(strippedExportMap);
        }

        GlobalOffsetTableElements_.Functions.clear();
        GlobalOffsetTableElements_.DataEntries.clear();
    }

    void* GetFunction(const std::string& name) override
    {
        for (const auto& it : Instances_) {
            if (auto* function = Runtime::asFunction(Runtime::getInstanceExport(it, name)); function != nullptr) {
                return static_cast<void*>(function);
            }
        }
        return nullptr;
    }

    void* GetFunction(size_t index) override
    {
        auto* tableElement = Runtime::getTableElement(GetGlobalOffsetTable(), std::bit_cast<Uptr>(index));
        return static_cast<void*>(Runtime::asFunction(tableElement));
    }

    void* GetContext() override
    {
        return static_cast<void*>(Context_);
    }

    uintptr_t AllocateBytes(size_t length) override
    {
        static const auto signature = IR::FunctionType(/*inResults*/ {IR::ValueType::i64}, /*inParams*/ {IR::ValueType::i64});
        auto* mallocFunction = Runtime::getTypedInstanceExport(RuntimeLibraryInstance_, "malloc", signature);
        auto arguments = std::array<IR::UntaggedValue, 1>{std::bit_cast<Uptr>(length)};
        auto result = IR::UntaggedValue{};
        SaveAndRestoreCompartment(this, [&] {
            Runtime::invokeFunction(Context_, mallocFunction, signature, arguments.data(), &result);
        });
        return result.u64;
    }

    void FreeBytes(uintptr_t offset) override
    {
        static const auto signature = IR::FunctionType(/*inResults*/ {}, /*inParams*/ {IR::ValueType::i64});
        auto* freeFunction = getTypedInstanceExport(RuntimeLibraryInstance_, "free", signature);
        auto arguments = std::array<IR::UntaggedValue, 1>{std::bit_cast<Uptr>(offset)};
        SaveAndRestoreCompartment(this, [&] {
            Runtime::invokeFunction(Context_, freeFunction, signature, arguments.data(), {});
        });
    }

    void SetTimeout(std::optional<TDuration> timeout) override
    {
        Timeout_ = timeout;
    }

    virtual void SetDeadline(std::optional<TInstant> deadline) override
    {
        if (!deadline || *deadline == TInstant::Max()) {
            Timeout_ = std::nullopt;
        } else {
            Timeout_ = *deadline - TInstant::Now();
        }
    }

    void StartDeadlineTimer() override
    {
        if (!Timeout_) {
            Deadline_ = std::nullopt;
        } else {
            Deadline_ = Runtime::getInstant();
            Deadline_->tv_sec += static_cast<time_t>(Timeout_->Seconds());
            Deadline_->tv_nsec += static_cast<long>(Timeout_->NanoSecondsOfSecond());
        }
    }

    std::optional<struct timespec> GetDeadline() const
    {
        return Deadline_;
    }

    void* GetHostPointer(uintptr_t offset, size_t length) override
    {
        char* bytes = Runtime::memoryArrayPtr<char>(MemoryLayoutData_.LinearMemory, std::bit_cast<ui64>(offset), length);
        return static_cast<void*>(bytes);
    }

    uintptr_t GetCompartmentOffset(void* hostAddress) override
    {
        ui64 hostAddressAsUint = std::bit_cast<ui64>(hostAddress);
        ui64 baseAddress = std::bit_cast<ui64>(Runtime::getMemoryBaseAddress(MemoryLayoutData_.LinearMemory));
        uintptr_t offset = hostAddressAsUint - baseAddress;
        return offset;
    }

    std::unique_ptr<IWebAssemblyCompartment> Clone() const override
    {
        auto result = std::unique_ptr<TWebAssemblyCompartment>(new TWebAssemblyCompartment());
        Clone(*this, result.get());
        return result;
    }

    Runtime::Memory* GetLinearMemory() const
    {
        return MemoryLayoutData_.LinearMemory;
    }

    Runtime::Table* GetGlobalOffsetTable() const
    {
        return MemoryLayoutData_.GlobalOffsetTable;
    }

    // This function is only declared here, because TLinker has not yet been defined.
    Runtime::LinkResult LinkModule(const IR::Module& irModule);

private:
    friend class TLinker;

    struct TLinkingData
    {
        std::vector<std::unique_ptr<Runtime::WeakFunction>> WeakFunctions;
        std::vector<std::pair<std::string, Uptr>> GlobalsToWeakFunctionsToPatch;
    };

    friend std::unique_ptr<TWebAssemblyCompartment> CreateImage(EKnownImage image);

    static constexpr int TypicalModuleCount = 5;

    Runtime::GCPointer<Runtime::Compartment> Compartment_;
    Runtime::GCPointer<Runtime::Context> Context_;

    Runtime::GCPointer<Runtime::Instance> IntrinsicsInstance_;
    Runtime::GCPointer<Runtime::Instance> RuntimeLibraryInstance_;
    TCompactVector<Runtime::GCPointer<Runtime::Instance>, 5> Instances_;

    TCompactVector<Runtime::ModuleRef, TypicalModuleCount> Modules_;

    TMemoryLayoutData MemoryLayoutData_;
    TLinkingData LinkingData_;
    TNamedGlobalOffsetTableElements GlobalOffsetTableElements_;

    Runtime::GCPointer<Runtime::ExceptionType> ExceptionType_;

    bool Stripped_ = false;

    std::optional<TDuration> Timeout_;
    std::optional<struct timespec> Deadline_;

    void AddExportsToGlobalOffsetTable(const IR::Module& irModule);
    void InstantiateModule(const Runtime::ModuleRef& wavmModule, const Runtime::LinkResult& linkResult, TStringBuf debugName);
    void ApplyDataRelocationsAndCallConstructors(Runtime::Instance* instance);

    static void Clone(const TWebAssemblyCompartment& source, TWebAssemblyCompartment* destination);
};

////////////////////////////////////////////////////////////////////////////////

class TLinker
    : public Runtime::Resolver
{
public:
    TLinker(TWebAssemblyCompartment* compartment, const IR::Module* incomingModule)
        : Compartment_(compartment)
        , IncomingModule_(incomingModule)
    { }

    bool resolve(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type,
        Runtime::Object*& outObject) override
    {
        if (auto result = ResolveMemoryLayoutGlobals(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveMisc(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveIntrinsics(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveFunctionFromGlobalOffsetTable(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveAlreadyLoadedFunctionAndInsertIntoGlobalOffsetTable(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveWeakFunction(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveMemoryFromGlobalOffsetTable(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveAlreadyLoadedObject(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveGlobalsPointingToMemory(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveGlobalsPointingToWeakFunctions(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        return false;
    }

private:
    TWebAssemblyCompartment* const Compartment_;
    const IR::Module* const IncomingModule_;

    std::optional<Runtime::Object*> ResolveMemoryLayoutGlobals(
        const std::string& /*moduleName*/,
        const std::string& objectName,
        IR::ExternType /*type*/)
    {
        if (objectName == "__linear_memory" || objectName == "memory") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.LinearMemory);
        } else if (objectName == "__indirect_function_table") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.GlobalOffsetTable);
        } else if (objectName == "__stack_pointer") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackPointer);
        } else if (objectName == "__heap_base") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.HeapBase);
        } else if (objectName == "__memory_base") {
            YT_VERIFY(std::ssize(Compartment_->MemoryLayoutData_.MemoryBases) == std::ssize(Compartment_->Modules_) + 1);
            Uptr newMemoryBase = Compartment_->MemoryLayoutData_.MemoryBases.back();
            auto* result = Runtime::createGlobal(Compartment_->Compartment_, IR::GlobalType{IR::ValueType::i64, false}, "__memory_base");
            Runtime::initializeGlobal(result, newMemoryBase);
            return Runtime::asObject(result);
        } else if (objectName == "__table_base") {
            YT_VERIFY(std::ssize(Compartment_->MemoryLayoutData_.TableBases) == std::ssize(Compartment_->Modules_) + 1);
            Uptr newTableBase = Compartment_->MemoryLayoutData_.TableBases.back();
            auto* result = Runtime::createGlobal(Compartment_->Compartment_, IR::GlobalType{IR::ValueType::i64, false}, "__table_base");
            Runtime::initializeGlobal(result, newTableBase);
            return Runtime::asObject(result);
        } else if (objectName == "__table_base32") {
            YT_VERIFY(std::ssize(Compartment_->MemoryLayoutData_.TableBases) == std::ssize(Compartment_->Modules_) + 1);
            Uptr newTableBase = Compartment_->MemoryLayoutData_.TableBases.back();
            auto* result = Runtime::createGlobal(Compartment_->Compartment_, IR::GlobalType{IR::ValueType::i32, false}, "__table_base");
            THROW_ERROR_EXCEPTION_IF(newTableBase > std::numeric_limits<I32>::max(), "WebAssembly linkage error: new table base is bigger than max i32 value");
            Runtime::initializeGlobal(result, static_cast<I32>(newTableBase));
            return Runtime::asObject(result);
        } else if (objectName == "__stack_low") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackLow);
        } else if (objectName == "__stack_high") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackHigh);
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveIntrinsics(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName == "env" || moduleName == "wasi_snapshot_preview1") {
            if (type.kind == IR::ExternKind::function) {
                auto* function = Runtime::getInstanceExport(Compartment_->IntrinsicsInstance_, objectName);
                if (function != nullptr) {
                    return function;
                }
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveFunctionFromGlobalOffsetTable(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName != "GOT.func") {
            return std::nullopt;
        }

        auto it = Compartment_->GlobalOffsetTableElements_.Functions.find(objectName);
        if (it == Compartment_->GlobalOffsetTableElements_.Functions.end()) {
            return std::nullopt;
        }

        Uptr indexInGOT = it->second;
        auto globalType = asGlobalType(type);
        globalType.isMutable = true;
        auto result = Runtime::createGlobal(Compartment_->Compartment_, globalType, std::string(objectName));
        Runtime::initializeGlobal(result, indexInGOT);
        return Runtime::asObject(result);
    }

    std::optional<Runtime::Object*> ResolveAlreadyLoadedFunctionAndInsertIntoGlobalOffsetTable(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName != "GOT.func") {
            return std::nullopt;
        }

        YT_ASSERT(!ResolveFunctionFromGlobalOffsetTable(moduleName, objectName, type).has_value());

        for (const auto& instance : Compartment_->Instances_) {
            auto* object = Runtime::getInstanceExport(instance, objectName);
            if (object != nullptr && object->kind == Runtime::ObjectKind::function) {
                Uptr indexInGOT = -1;
                auto growResult = Runtime::growTable(Compartment_->GetGlobalOffsetTable(), 1, &indexInGOT);
                THROW_ERROR_EXCEPTION_IF(growResult != Runtime::GrowResult::success, "WebAssembly grow GOT error");
                Runtime::setTableElement(Compartment_->GetGlobalOffsetTable(), indexInGOT, object);
                Compartment_->GlobalOffsetTableElements_.Functions[objectName] = indexInGOT;
                return ResolveFunctionFromGlobalOffsetTable(moduleName, objectName, type);
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveWeakFunction(
        const std::string& /*moduleName*/,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (type.kind == IR::ExternKind::function) {
            for (const auto& incomingModuleExport : IncomingModule_->exports) {
                if (incomingModuleExport.name == objectName) {
                    auto name = std::string("wasm!env!" + CppDemangle(TString(objectName)));
                    Compartment_->LinkingData_.WeakFunctions.emplace_back(std::move(std::make_unique<Runtime::WeakFunction>(
                        std::move(name),
                        incomingModuleExport.index)));
                    return Compartment_->LinkingData_.WeakFunctions.back().get();
                }
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveMemoryFromGlobalOffsetTable(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName != "GOT.mem") {
            return std::nullopt;
        }

        auto demangled = CppDemangle(TString(objectName));

        auto it = Compartment_->GlobalOffsetTableElements_.DataEntries.find(demangled);
        if (it == Compartment_->GlobalOffsetTableElements_.DataEntries.end()) {
            return std::nullopt;
        }

        I64 indexInGOT = it->second;
        auto globalType = asGlobalType(type);
        globalType.isMutable = true;
        auto result = Runtime::createGlobal(Compartment_->Compartment_, globalType, std::string(demangled));
        YT_ASSERT(result != nullptr);
        Runtime::initializeGlobal(result, indexInGOT);
        return Runtime::asObject(result);
    }

    std::optional<Runtime::Object*> ResolveAlreadyLoadedObject(
        const std::string& /*moduleName*/,
        const std::string& objectName,
        IR::ExternType type)
    {
        for (const auto& instance : Compartment_->Instances_) {
            auto* object = Runtime::getInstanceExport(instance, objectName);
            if (object != nullptr && static_cast<U8>(object->kind) == static_cast<U8>(type.kind)) {
                return object;
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveMisc(
        const std::string& /*moduleName*/,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (objectName == "__cpp_exception") {
            return Compartment_->ExceptionType_;
        }

        if (objectName == "emscripten_console_trace") {
            auto globalType = asGlobalType(type);
            globalType.isMutable = true;
            auto result = Runtime::createGlobal(
                Compartment_->Compartment_,
                globalType,
                std::string(objectName));
            Runtime::initializeGlobal(Runtime::asGlobal(result), static_cast<I64>(0));
            return result;
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveGlobalsPointingToWeakFunctions(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (type.kind != IR::ExternKind::global) {
            return std::nullopt;
        }

        for (const auto& incomingModuleExport : IncomingModule_->exports) {
            if (incomingModuleExport.kind == WAVM::IR::ExternKind::function && incomingModuleExport.name == objectName) {
                Uptr indexInGOT = -1;
                auto growResult = Runtime::growTable(Compartment_->GetGlobalOffsetTable(), 1, &indexInGOT);
                THROW_ERROR_EXCEPTION_IF(growResult != Runtime::GrowResult::success, "WebAssembly grow GOT error");
                Compartment_->LinkingData_.GlobalsToWeakFunctionsToPatch.emplace_back(objectName, indexInGOT);
                Compartment_->GlobalOffsetTableElements_.Functions[objectName] = indexInGOT;
                return ResolveFunctionFromGlobalOffsetTable(moduleName, objectName, type);
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveGlobalsPointingToMemory(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (type.kind != IR::ExternKind::global || moduleName != "GOT.mem") {
            return std::nullopt;
        }

        for (const auto& exportedDataEntry : IncomingModule_->exports) {
            if (exportedDataEntry.kind != IR::ExternKind::global) {
                continue;
            }
            if (exportedDataEntry.name == objectName) {
                auto globalType = asGlobalType(type);
                globalType.isMutable = true;
                auto outObject = Runtime::createGlobal(
                    Compartment_->Compartment_,
                    globalType,
                    std::string(objectName));
                auto incomingGlobal = IncomingModule_->globals.defs[exportedDataEntry.index - IncomingModule_->globals.imports.size()];
                Runtime::initializeGlobal(Runtime::asGlobal(outObject), incomingGlobal.initializer.i64);
                return outObject;
            }
        }

        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

Runtime::LinkResult TWebAssemblyCompartment::LinkModule(const IR::Module& irModule)
{
    auto linker = TLinker(this, &irModule);
    auto linkResult = Runtime::linkModule(irModule, linker);

    if (!linkResult.success) {
        THROW_ERROR_EXCEPTION(
            "WebAssembly linkage error. Missing: %v",
            MakeFormattableView(
                linkResult.missingImports,
                [] (TStringBuilderBase* builder, const auto& missingImport) {
                    FormatValue(builder, missingImport.exportName, "v");
                }));
    }

    return linkResult;
}

void TWebAssemblyCompartment::AddExportsToGlobalOffsetTable(const IR::Module& irModule)
{
    IR::DisassemblyNames disassemblyNames;
    getDisassemblyNames(irModule, disassemblyNames);

    auto exportedFunctions = THashSet<std::string>();
    for (const auto& item : irModule.exports) {
        if (item.kind == IR::ExternKind::function) {
            exportedFunctions.insert(CppDemangle(TString(item.name)));
        }
    }

    Uptr baseOffset = Runtime::getTableNumElements(GetGlobalOffsetTable());
    for (const auto& elementSegment : irModule.elemSegments) {
        if (elementSegment.type != IR::ElemSegment::Type::active) {
            continue;
        }
        for (int index = 0; index < std::ssize(elementSegment.contents->elemIndices); index++) {
            int functionIndex = elementSegment.contents->elemIndices[index];
            auto& functionName = disassemblyNames.functions[functionIndex].name;
            if (exportedFunctions.contains(functionName)) {
                int globalOffsetTableIndex = baseOffset + index;
                GlobalOffsetTableElements_.Functions[functionName] = globalOffsetTableIndex;
            }
        }
    }

    for (const auto& exportedDataEntry : irModule.exports) {
        if (exportedDataEntry.kind != IR::ExternKind::global) {
            continue;
        }

        const auto& global = irModule.globals.getDef(exportedDataEntry.index);
        i64 offset = 0;
        i64 value = offset + global.initializer.i64;
        auto demangled = CppDemangle(TString(exportedDataEntry.name));
        GlobalOffsetTableElements_.DataEntries[demangled] = value;
    }
}

void TWebAssemblyCompartment::InstantiateModule(
    const Runtime::ModuleRef& wavmModule,
    const Runtime::LinkResult& linkResult,
    TStringBuf debugName)
{
    YT_VERIFY(linkResult.success);

    {
        const Uptr tableBase = MemoryLayoutData_.TableBases.back();
        Uptr requiredTableSize = tableBase;
        for (const auto& elemSegment : wavmModule->ir.elemSegments) {
            requiredTableSize = std::max(
                requiredTableSize,
                tableBase + GetActiveElemSegmentSize(elemSegment));
        }
        EnsureGlobalOffsetTableCapacity(GetGlobalOffsetTable(), requiredTableSize);
    }

    Runtime::Instance* instance = nullptr;
    try {
        instance = Runtime::instantiateModule(
            Compartment_,
            wavmModule,
            Runtime::ImportBindings{linkResult.resolvedImports},
            debugName.data());
    } catch (WAVM::Runtime::Exception* ex) {
        const auto description = WAVM::Runtime::describeException(ex);
        WAVM::Runtime::destroyException(ex);
        THROW_ERROR_EXCEPTION("WebAssembly instantiate module failed: %Qv", description);
    }
    THROW_ERROR_EXCEPTION_IF(instance == nullptr, "WebAssembly instantiate module failed");
    Modules_.push_back(wavmModule);
    Instances_.push_back(instance);

    {
        Uptr lastMemoryBase = MemoryLayoutData_.MemoryBases.back();
        Uptr newMemoryBase = lastMemoryBase;
        for (auto& dataSegment : wavmModule->ir.dataSegments) {
            newMemoryBase += dataSegment.data->size();
        }
        MemoryLayoutData_.MemoryBases.push_back(newMemoryBase);
    }

    {
        Uptr lastTableBase = MemoryLayoutData_.TableBases.back();
        Uptr newTableBase = lastTableBase;
        for (auto& elemSegment : wavmModule->ir.elemSegments) {
            THROW_ERROR_EXCEPTION_IF(elemSegment.tableIndex != 0, "Unsupported module: elem segment table index must be 0");
            newTableBase += GetActiveElemSegmentSize(elemSegment);
        }
        MemoryLayoutData_.TableBases.push_back(newTableBase);
    }

    {
        LinkingData_.WeakFunctions.clear();
    }

    {
        for (auto& [name, indexInGOT] : LinkingData_.GlobalsToWeakFunctionsToPatch) {
            auto* object = Runtime::getInstanceExport(instance, name);
            THROW_ERROR_EXCEPTION_IF(object == nullptr, "WebAssembly linkage error: could not find object %Qv in the instantiated module", name);
            Runtime::setTableElement(GetGlobalOffsetTable(), indexInGOT, object);
        }
        LinkingData_.GlobalsToWeakFunctionsToPatch.clear();
    }

    ApplyDataRelocationsAndCallConstructors(instance);
}

void TWebAssemblyCompartment::ApplyDataRelocationsAndCallConstructors(Runtime::Instance* instance)
{
    auto callIfDefined = [this] (Runtime::Instance* instance, const IR::FunctionType& signature, const std::string& name) {
        if (auto* function = getTypedInstanceExport(instance, name, signature)) {
            auto arguments = std::array<IR::UntaggedValue, 0>{};
            SaveAndRestoreCompartment(this, [&] {
                try {
                    Runtime::invokeFunction(Context_, function, signature, arguments.data(), {});
                } catch (WAVM::Runtime::Exception* ex) {
                    auto description = WAVM::Runtime::describeException(ex);
                    WAVM::Runtime::destroyException(ex);
                    THROW_ERROR_EXCEPTION("WAVM Runtime Exception: %Qv", description);
                }
            });
        }
    };

    static const auto VoidToVoidSignature = IR::FunctionType(/*inResults*/ {}, /*inParams*/ {});
    callIfDefined(instance, VoidToVoidSignature, "__wasm_apply_data_relocs");
    callIfDefined(instance, VoidToVoidSignature, "__wasm_apply_global_relocs");
    callIfDefined(instance, VoidToVoidSignature, "__wasm_call_ctors");
}

void TWebAssemblyCompartment::Clone(const TWebAssemblyCompartment& source, TWebAssemblyCompartment* destination)
{
    destination->Compartment_ = Runtime::cloneCompartment(source.Compartment_);
    destination->Context_ = Runtime::cloneContext(source.Context_, destination->Compartment_);

    YT_ASSERT(destination->Compartment_->instances.size() >= 1);

    destination->IntrinsicsInstance_ = *destination->Compartment_->instances.get(0);
    destination->Instances_.push_back(destination->IntrinsicsInstance_);

    if (source.RuntimeLibraryInstance_) {
        destination->RuntimeLibraryInstance_ = *destination->Compartment_->instances.get(1);
        destination->Instances_.push_back(destination->RuntimeLibraryInstance_);
    }

    for (int index = 2; index < std::ssize(destination->Compartment_->instances); ++index) {
        destination->Instances_.push_back(*destination->Compartment_->instances.get(index));
    }

    destination->MemoryLayoutData_.LinearMemory = *destination->Compartment_->memories.get(0);
    destination->MemoryLayoutData_.GlobalOffsetTable = *destination->Compartment_->tables.get(0);
    destination->GlobalOffsetTableElements_ = source.GlobalOffsetTableElements_;

    destination->MemoryLayoutData_.MemoryBases = source.MemoryLayoutData_.MemoryBases;
    destination->MemoryLayoutData_.TableBases = source.MemoryLayoutData_.TableBases;
    destination->Modules_ = source.Modules_;

    if (source.ExceptionType_) {
        destination->ExceptionType_ = destination->Compartment_->exceptionTypes[source.ExceptionType_->id];
    }

    for (auto* global : destination->Compartment_->globals) {
        if (global->debugName == "__stack_pointer") {
            destination->MemoryLayoutData_.StackPointer = global;
        } else if (global->debugName == "__heap_base") {
            destination->MemoryLayoutData_.HeapBase = global;
        } else if (global->debugName == "__memory_base") {
            destination->MemoryLayoutData_.MemoryBase = global;
        } else if (global->debugName == "__table_base") {
            destination->MemoryLayoutData_.TableBase = global;
        } else if (global->debugName == "__table_base32") {
            destination->MemoryLayoutData_.TableBase32 = global;
        } else if (global->debugName == "__stack_low") {
            destination->MemoryLayoutData_.StackLow = global;
        } else if (global->debugName == "__stack_high") {
            destination->MemoryLayoutData_.StackHigh = global;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Runtime::ModuleRef LoadMinimalRuntimeSdk()
{
    auto bytecode = GetBuiltinMinimalRuntimeSdk();
    YT_VERIFY(bytecode.Format == EBytecodeFormat::HumanReadable);
    return Runtime::compileModule(ParseWast(TString(bytecode.Data.ToStringBuf())));
}

Runtime::ModuleRef LoadBuiltinSdk()
{
    auto featureSpec = IR::FeatureSpec();
    featureSpec.memory64 = true;
    featureSpec.exceptionHandling = true;
    auto irModule = IR::Module(std::move(featureSpec));

    auto bytecode = GetBuiltinSdk();

    auto loadError = WASM::LoadError();
    bool succeeded = WASM::loadBinaryModule(
        std::bit_cast<U8*>(bytecode.Data.begin()),
        bytecode.Data.size(),
        irModule,
        &loadError);

    if (!succeeded) {
        THROW_ERROR_EXCEPTION("Could not load WebAssembly system libraries: %v", loadError.message);
    }

    auto objectCode = std::vector<U8>(bytecode.ObjectCode.size());
    ::memcpy(objectCode.data(), bytecode.ObjectCode.data(), bytecode.ObjectCode.size());

    return std::make_shared<Runtime::Module>(std::move(irModule), std::move(objectCode));
}

Runtime::ModuleRef LoadBuiltinUdfs()
{
    auto bytecode = GetBuiltinYtQlUdfs();
    return LoadModuleFromBytecode(bytecode.Data);
}

namespace {

bool CheckFreeStackSpace(size_t /*space*/)
{
    return true;
}

void CheckStackDepth()
{
    static const int MinimumStackFreeSpace = 8_KB;

    if (!CheckFreeStackSpace(MinimumStackFreeSpace)) {
        THROW_ERROR_EXCEPTION("Expression depth causes stack overflow");
    }
}

} // namespace

std::unique_ptr<TWebAssemblyCompartment> CreateImage(EKnownImage image)
{
    auto compartment = std::make_unique<TWebAssemblyCompartment>();
    compartment->Compartment_ = Runtime::createCompartment();
    compartment->Context_ = Runtime::createContext(compartment->Compartment_);
    Runtime::setCheckStackDepthCallback(compartment->Context_, CheckStackDepth);

    compartment->MemoryLayoutData_ = BuildMemoryLayoutData(compartment->Compartment_);

    if (image == EKnownImage::MinimalRuntime) {
        compartment->IntrinsicsInstance_ = Intrinsics::instantiateModule(
            compartment->Compartment_,
            {WAVM_INTRINSIC_MODULE_REF(empty)},
            "env");
    } else {
        compartment->IntrinsicsInstance_ = Intrinsics::instantiateModule(
            compartment->Compartment_,
            {WAVM_INTRINSIC_MODULE_REF(standard)},
            "env");
    }

    if (image != EKnownImage::MinimalRuntime) {
        compartment->ExceptionType_ = Runtime::createExceptionType(
            compartment->Compartment_,
            IR::ExceptionType{IR::TypeTuple{IR::ValueType::i64}},
            "__cpp_exception");
    }

    auto runtimeModule = Runtime::ModuleRef();
    switch (image) {
        case EKnownImage::Empty:
            break;
        case EKnownImage::MinimalRuntime:
            runtimeModule = LoadMinimalRuntimeSdk();
            break;
        case EKnownImage::Standard:
        case EKnownImage::QueryLanguage:
            runtimeModule = LoadBuiltinSdk();
            break;
        default:
            YT_ABORT();
    }

    if (runtimeModule) {
        const auto& runtimeIR = Runtime::getModuleIR(runtimeModule);
        auto linkResult = compartment->LinkModule(runtimeIR);
        compartment->AddExportsToGlobalOffsetTable(runtimeIR);
        compartment->InstantiateModule(runtimeModule, linkResult, "env");
        compartment->RuntimeLibraryInstance_ = compartment->Instances_.back();
    }

    if (image == EKnownImage::QueryLanguage) {
        auto wasmModule = LoadBuiltinUdfs();
        const auto& irModule = Runtime::getModuleIR(wasmModule);
        auto linkResult = compartment->LinkModule(irModule);
        compartment->AddExportsToGlobalOffsetTable(irModule);
        compartment->InstantiateModule(wasmModule, linkResult, "env");
    }

    return compartment;
}

////////////////////////////////////////////////////////////////////////////////

struct TCachedSdkImage
    : public NYT::TRefCounted
{
    std::unique_ptr<IWebAssemblyCompartment> Compartment;

    explicit TCachedSdkImage(std::unique_ptr<IWebAssemblyCompartment> compartment)
        : Compartment(std::move(compartment))
    { }
};

using TCachedSdkImagePtr = NYT::TIntrusivePtr<TCachedSdkImage>;

class TSdkImageCache
    : public NYT::TRefCounted
{
public:
    static constexpr size_t DefaultCapacity = 5;

    TCachedSdkImagePtr GetOrCreate(const TModuleBytecode& bytecode)
    {
        with_lock (Lock_) {
            if (auto it = Cache_.find(bytecode)) {
                return it->second;
            }
        }

        auto compartment = CreateEmptyImage();
        compartment->AddSdk(bytecode);
        auto cachedImage = New<TCachedSdkImage>(std::move(compartment));

        with_lock (Lock_) {
            if (Cache_.size() >= DefaultCapacity && !Cache_.empty()) {
                Cache_.erase(Cache_.begin());
            }
            Cache_[bytecode] = cachedImage;
        }

        return cachedImage;
    }

private:
    TMutex Lock_;
    THashMap<TModuleBytecode, TCachedSdkImagePtr> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWebAssemblyCompartment> CreateImageFromSdk(const TModuleBytecode& bytecode)
{
    if (bytecode.Data.ToStringBuf() == GetBuiltinSdk().Data.ToStringBuf()) {
        return CreateStandardRuntimeImage();
    }

    static auto cache = New<TSdkImageCache>();
    auto image = cache->GetOrCreate(bytecode);
    return image->Compartment->Clone();
}

std::unique_ptr<IWebAssemblyCompartment> CreateEmptyImage()
{
    static std::unique_ptr<TWebAssemblyCompartment> leakyImageSingleton = CreateImage(EKnownImage::Empty);
    return leakyImageSingleton->Clone();
}

std::unique_ptr<IWebAssemblyCompartment> CreateMinimalRuntimeImage()
{
    static std::unique_ptr<TWebAssemblyCompartment> leakyImageSingleton = CreateImage(EKnownImage::MinimalRuntime);
    return leakyImageSingleton->Clone();
}

std::unique_ptr<IWebAssemblyCompartment> CreateStandardRuntimeImage()
{
    THROW_ERROR_EXCEPTION_IF(!EnableSystemLibraries(), "WebAssembly runtime libraries are not supported by this build");

    static std::unique_ptr<TWebAssemblyCompartment> leakyImageSingleton = CreateImage(EKnownImage::Standard);
    return leakyImageSingleton->Clone();
}

std::unique_ptr<IWebAssemblyCompartment> CreateQueryLanguageImage()
{
    THROW_ERROR_EXCEPTION_IF(!EnableSystemLibraries(), "WebAssembly runtime libraries are not supported by this build");

    static std::unique_ptr<TWebAssemblyCompartment> leakyImageSingleton = CreateImage(EKnownImage::QueryLanguage);
    return leakyImageSingleton->Clone();
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(IWebAssemblyCompartment*, CurrentCompartment);

IWebAssemblyCompartment* GetCurrentCompartment()
{
    return CurrentCompartment();
}

void SetCurrentCompartment(IWebAssemblyCompartment* compartment)
{
    CurrentCompartment() = compartment;
    if (compartment) {
        Runtime::Table::setCurrentTable(
            static_cast<TWebAssemblyCompartment*>(compartment)->GetGlobalOffsetTable());
        Runtime::Memory::setCurrentMemory(
            static_cast<TWebAssemblyCompartment*>(compartment)->GetLinearMemory());
        Runtime::setCurrentDeadline(
            static_cast<TWebAssemblyCompartment*>(compartment)->GetDeadline());
    } else {
        Runtime::Table::setCurrentTable(nullptr);
        Runtime::Memory::setCurrentMemory(nullptr);
        Runtime::setCurrentDeadline(std::nullopt);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
