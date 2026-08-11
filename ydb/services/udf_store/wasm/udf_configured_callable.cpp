#include "udf_configured_callable.h"

#include "compartment_manager.h"
#include "invocation_context.h"
#include "registry_helpers.h"
#include "udf_function.h"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <bit>
#include <cstring>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;

namespace {

void WasmError(const std::exception& ex, TStringRef name, const IValueBuilder* valueBuilder) {
    Y_UNUSED(valueBuilder);
    UdfTerminate((TStringBuilder() << name << "(); ex: " << ex.what()).c_str());
}

struct TEphemeralValue {
    TCopyGuard Guard;
    uintptr_t Offset = 0;
};

TEphemeralValue MakeEphemeralValue(
    IWebAssemblyCompartment* compartment,
    const TUnversionedValue& value)
{
    TEphemeralValue prepared;
    prepared.Offset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    prepared.Guard = TCopyGuard(compartment, prepared.Offset);
    StoreValue(compartment, prepared.Offset, value);
    return prepared;
}

} // namespace

void TWasmConfiguredCallable::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor,
    TString typeConfigBlob)
{
    builder.Returns(TWasmUdfFunction::BuildYqlType(builder, descriptor.Result));
    auto args = builder.Args(descriptor.Args.size());
    for (const auto argType : descriptor.Args) {
        args->Add(TWasmUdfFunction::BuildYqlType(builder, argType));
    }
    args->Done();

    if (!typesOnly) {
        builder.Implementation(new TWasmConfiguredCallable(
            std::move(state),
            descriptor,
            std::move(typeConfigBlob)));
    }
}

TWasmConfiguredCallable::TWasmConfiguredCallable(
    TWasmCompartmentStatePtr state,
    TWasmUdfDescriptor descriptor,
    TString typeConfigBlob)
    : State_(std::move(state))
    , Descriptor_(std::move(descriptor))
    , ConfigBlob_(std::move(typeConfigBlob))
{
}

TWasmConfiguredCallable::~TWasmConfiguredCallable() {
    try {
        DestroyObjectIfAlive();
    } catch (...) {
        // Best-effort cleanup; never throw from destructor.
    }
}

void TWasmConfiguredCallable::DestroyObjectIfAlive() const {
    const ui64 handle = Handle_;
    const ui64 generation = CompartmentGeneration_;
    Handle_ = 0;
    CompartmentGeneration_ = 0;
    PinnedBlobOffset_ = 0;
    PinnedBlobLength_ = 0;

    if (handle == 0 || Descriptor_.DestroyExport.empty()) {
        return;
    }

    auto* queryHandle = GetCurrentQueryCompartment();
    if (!queryHandle || !queryHandle->Compartment
        || queryHandle->Generation != generation)
    {
        // Old compartment is already gone (or TLS not installed): guest
        // ObjectFramework / malloc state is freed with the compartment.
        return;
    }

    auto* compartment = queryHandle->Compartment.get();
    TCurrentCompartmentGuard compartmentGuard(compartment);
    TWasmUdfInvocationContext context(compartment);
    TCurrentInvocationContextGuard invocationGuard(&context);

    TUnversionedValue handleValue = MakeEmptyValue();
    handleValue.Type = EAbiValueType::Uint64;
    handleValue.Data.Uint64 = handle;
    auto handleSlot = MakeEphemeralValue(compartment, handleValue);
    auto resultSlot = MakeEphemeralValue(compartment, MakeEmptyValue());

    const auto destroyKey = MakeExportKey(State_->ModuleName, Descriptor_.DestroyExport);
    if (auto* destroyExport = queryHandle->Exports.FindPtr(destroyKey)) {
        InvokeUdfExport(
            compartment,
            *destroyExport,
            Descriptor_.DestroyExport,
            std::bit_cast<uintptr_t>(&context),
            resultSlot.Offset,
            {handleSlot.Offset});
    }
}

void TWasmConfiguredCallable::EnsureObject(TStringRef functionNameForErrors) const {
    auto* queryHandle = GetCurrentQueryCompartment();
    Y_ENSURE(queryHandle && queryHandle->Compartment,
        "Query WASM compartment is not initialized");
    Y_ENSURE(queryHandle->Generation != 0, "Query WASM compartment generation is unset");

    if (Handle_ != 0 && CompartmentGeneration_ == queryHandle->Generation) {
        return;
    }

    // Drop the previous guest object if the compartment is still the same
    // generation (defensive). Across generation changes DestroyObjectIfAlive
    // is a no-op — the old compartment (and its ObjectFramework registry) is gone.
    DestroyObjectIfAlive();

    auto* compartment = queryHandle->Compartment.get();
    TCurrentCompartmentGuard compartmentGuard(compartment);
    TWasmUdfInvocationContext context(compartment);
    TCurrentInvocationContextGuard invocationGuard(&context);

    if (PinnedBlobOffset_ == 0 && !ConfigBlob_.empty()) {
        PinnedBlobOffset_ = compartment->AllocateBytes(ConfigBlob_.size());
        auto* dst = PtrFromVM(
            compartment,
            std::bit_cast<char*>(PinnedBlobOffset_),
            ConfigBlob_.size());
        std::memcpy(dst, ConfigBlob_.data(), ConfigBlob_.size());
        PinnedBlobLength_ = static_cast<ui32>(ConfigBlob_.size());
    }

    TUnversionedValue configValue = MakeEmptyValue();
    configValue.Type = EAbiValueType::String;
    configValue.Length = PinnedBlobLength_;
    configValue.Data.String = PinnedBlobLength_ == 0
        ? nullptr
        : std::bit_cast<char*>(PinnedBlobOffset_);

    auto configArg = MakeEphemeralValue(compartment, configValue);
    auto resultSlot = MakeEphemeralValue(compartment, MakeEmptyValue());

    const auto createKey = MakeExportKey(State_->ModuleName, Descriptor_.CreateExport);
    auto* createExport = queryHandle->Exports.FindPtr(createKey);
    Y_ENSURE(createExport, "Missing WASM create export binding for " << createKey);

    InvokeUdfExport(
        compartment,
        *createExport,
        Descriptor_.CreateExport,
        std::bit_cast<uintptr_t>(&context),
        resultSlot.Offset,
        {configArg.Offset});

    const auto created = *PtrFromVM(
        compartment,
        std::bit_cast<TUnversionedValue*>(resultSlot.Offset));
    if (created.Type != EAbiValueType::Int64 && created.Type != EAbiValueType::Uint64) {
        ythrow yexception()
            << functionNameForErrors
            << ": create export must return int64/uint64 handle, got type "
            << static_cast<int>(created.Type);
    }
    const ui64 handle = created.Type == EAbiValueType::Int64
        ? static_cast<ui64>(created.Data.Int64)
        : created.Data.Uint64;
    if (handle == 0) {
        ythrow yexception() << functionNameForErrors << ": create export returned null handle";
    }

    Handle_ = handle;
    CompartmentGeneration_ = queryHandle->Generation;
}

TUnboxedValue TWasmConfiguredCallable::Run(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
    try {
        EnsureObject(TStringRef(Descriptor_.Name));

        auto* queryHandle = GetCurrentQueryCompartment();
        auto* compartment = queryHandle->Compartment.get();
        compartment->SetTimeout(TDuration::Minutes(1));
        compartment->StartDeadlineTimer();
        TCurrentCompartmentGuard compartmentGuard(compartment);
        TWasmUdfInvocationContext context(compartment);
        TCurrentInvocationContextGuard invocationGuard(&context);

        TUnversionedValue handleValue = MakeEmptyValue();
        handleValue.Type = EAbiValueType::Uint64;
        handleValue.Data.Uint64 = Handle_;
        auto handleSlot = MakeEphemeralValue(compartment, handleValue);

        TVector<TCopyGuard> stringGuards;
        stringGuards.reserve(Descriptor_.Args.size());
        TVector<TEphemeralValue> argSlots;
        argSlots.reserve(1 + Descriptor_.Args.size());
        TVector<uintptr_t> argOffsets;
        argOffsets.reserve(1 + Descriptor_.Args.size());
        argOffsets.push_back(handleSlot.Offset);

        for (size_t i = 0; i < Descriptor_.Args.size(); ++i) {
            TUnversionedValue value = MakeEmptyValue();
            const auto& arg = args[i];
            if (!arg) {
                value.Type = EAbiValueType::Null;
            } else {
                switch (Descriptor_.Args[i]) {
                    case EUdfValueType::Int64:
                        value.Type = EAbiValueType::Int64;
                        value.Data.Int64 = arg.Get<i64>();
                        break;
                    case EUdfValueType::Uint64:
                        value.Type = EAbiValueType::Uint64;
                        value.Data.Uint64 = arg.Get<ui64>();
                        break;
                    case EUdfValueType::Double:
                        value.Type = EAbiValueType::Double;
                        value.Data.Double = arg.Get<double>();
                        break;
                    case EUdfValueType::Boolean:
                        value.Type = EAbiValueType::Boolean;
                        value.Data.Boolean = arg.Get<bool>() ? 1 : 0;
                        break;
                    case EUdfValueType::String: {
                        const TStringBuf string = arg.AsStringRef();
                        stringGuards.push_back(CopyIntoCompartment(string, compartment));
                        value.Type = EAbiValueType::String;
                        value.Length = static_cast<ui32>(string.size());
                        value.Data.String = std::bit_cast<char*>(
                            stringGuards.back().GetCopiedOffset());
                        break;
                    }
                    case EUdfValueType::Null:
                        value.Type = EAbiValueType::Null;
                        break;
                }
            }
            argSlots.push_back(MakeEphemeralValue(compartment, value));
            argOffsets.push_back(argSlots.back().Offset);
        }

        auto resultSlot = MakeEphemeralValue(compartment, MakeEmptyValue());
        const auto callKey = MakeExportKey(State_->ModuleName, Descriptor_.CallExport);
        auto* callExport = queryHandle->Exports.FindPtr(callKey);
        Y_ENSURE(callExport, "Missing WASM call export binding for " << callKey);

        InvokeUdfExport(
            compartment,
            *callExport,
            Descriptor_.CallExport,
            std::bit_cast<uintptr_t>(&context),
            resultSlot.Offset,
            argOffsets);

        const auto result = *PtrFromVM(
            compartment,
            std::bit_cast<TUnversionedValue*>(resultSlot.Offset));
        if (result.Type == EAbiValueType::Null) {
            return {};
        }
        switch (Descriptor_.Result) {
            case EUdfValueType::Int64:
                return TUnboxedValuePod(result.Data.Int64);
            case EUdfValueType::Uint64:
                return TUnboxedValuePod(result.Data.Uint64);
            case EUdfValueType::Double:
                return TUnboxedValuePod(result.Data.Double);
            case EUdfValueType::Boolean:
                return TUnboxedValuePod(result.Data.Boolean != 0);
            case EUdfValueType::String: {
                const auto* hostData = PtrFromVM(
                    compartment,
                    result.Data.String,
                    result.Length);
                return valueBuilder->NewString(TStringRef(
                    reinterpret_cast<const char*>(hostData),
                    result.Length));
            }
            case EUdfValueType::Null:
                return {};
        }
        return {};
    } catch (const std::exception& ex) {
        WasmError(ex, TStringRef(Descriptor_.Name), valueBuilder);
    }
    return {};
}

} // namespace NKikimr::NUdfStore::NWasm
