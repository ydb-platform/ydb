#include "udf_function.h"

#include "compartment_manager.h"
#include "invocation_context.h"
#include "registry_helpers.h"
#include "udf_configured_callable.h"
#include "wasm_string.h"

#include <yql/essentials/public/udf/udf_type_builder.h>

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>
#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;
using EAbiValueType = NYdb::NUdfStore::NAbi::EValueType;
using EAbiValueFlags = NYdb::NUdfStore::NAbi::EValueFlags;

namespace {

void WasmError(const std::exception& ex, TStringRef name, const IValueBuilder* valueBuilder) {
    Y_UNUSED(valueBuilder);
    const auto msg = TStringBuilder() << name << "(); ex: " << ex.what();
    UdfTerminate(msg.c_str());
}

} // namespace

namespace {

struct TPreparedArg {
    TPreparedUdfArg Storage;
};

TPreparedArg PrepareArgFromUnboxed(
    IWebAssemblyCompartment* compartment,
    const TUnboxedValuePod& arg,
    EUdfValueType expectedType)
{
    TPreparedArg prepared;
    auto value = MakeEmptyValue();

    if (!arg) {
        value.Type = EAbiValueType::Null;
    } else {
        switch (expectedType) {
            case EUdfValueType::Null:
                value.Type = EAbiValueType::Null;
                break;
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
                TWasmStringValue::FillAbiStringArg(
                    compartment, arg, value, prepared.Storage.StringGuard);
                break;
            }
        }
    }

    const auto offset = compartment->AllocateBytes(sizeof(TUnversionedValue));
    prepared.Storage.ValueGuard = TCopyGuard(compartment, offset);
    prepared.Storage.Offset = offset;
    StoreValue(compartment, offset, value);
    return prepared;
}

} // namespace

TUnboxedValue ReadResultUnboxed(
    const IValueBuilder* valueBuilder,
    IWebAssemblyCompartment* compartment,
    uintptr_t resultOffset,
    EUdfValueType expectedType)
{
    const auto result = *PtrFromVM(compartment, std::bit_cast<TUnversionedValue*>(resultOffset));
    if (result.Type == EAbiValueType::Null) {
        return {};
    }

    switch (expectedType) {
        case EUdfValueType::Null:
            return {};
        case EUdfValueType::Int64:
            if (result.Type != EAbiValueType::Int64) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Int64 result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Int64);
        case EUdfValueType::Uint64:
            if (result.Type != EAbiValueType::Uint64) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Uint64 result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Uint64);
        case EUdfValueType::Double:
            if (result.Type != EAbiValueType::Double) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Double result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(result.Data.Double);
        case EUdfValueType::Boolean:
            if (result.Type != EAbiValueType::Boolean) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for Boolean result: "
                    << static_cast<int>(result.Type);
            }
            return TUnboxedValuePod(static_cast<bool>(result.Data.Boolean));
        case EUdfValueType::String: {
            if (result.Type != EAbiValueType::String) {
                ythrow yexception()
                    << "Wasm UDF returned wrong value type for String result: "
                    << static_cast<int>(result.Type);
            }
            const auto* hostData = PtrFromVM(compartment, result.Data.String, result.Length);
            return valueBuilder->NewString(TStringRef(hostData, result.Length));
        }
    }

    return {};
}

TType* TWasmUdfFunction::BuildYqlType(IFunctionTypeInfoBuilder& builder, EUdfValueType type) {
    switch (type) {
        case EUdfValueType::Null:
            return builder.Null();
        case EUdfValueType::Int64:
            return builder.Optional()->Item<i64>().Build();
        case EUdfValueType::Uint64:
            return builder.Optional()->Item<ui64>().Build();
        case EUdfValueType::Double:
            return builder.Optional()->Item<double>().Build();
        case EUdfValueType::Boolean:
            return builder.Optional()->Item<bool>().Build();
        case EUdfValueType::String:
            return builder.Optional()->Item<char*>().Build();
    }
    return builder.Null();
}

TType* TWasmUdfFunction::BuildFunctionType(
    IFunctionTypeInfoBuilder& builder,
    const TWasmUdfDescriptor& descriptor)
{
    auto callable = builder.Callable(descriptor.Args.size());
    callable->Returns(BuildYqlType(builder, descriptor.Result));
    for (const auto argType : descriptor.Args) {
        callable->Arg(BuildYqlType(builder, argType));
    }
    return callable->Build();
}

void TWasmUdfFunction::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor)
{
    builder.Returns(BuildYqlType(builder, descriptor.Result));
    auto args = builder.Args(descriptor.Args.size());
    for (const auto argType : descriptor.Args) {
        args->Add(BuildYqlType(builder, argType));
    }

    if (!typesOnly) {
        builder.Implementation(new TWasmUdfFunction(std::move(state), descriptor));
    }
}

TWasmUdfFunction::TWasmUdfFunction(TWasmCompartmentStatePtr state, const TWasmUdfDescriptor& descriptor)
    : State_(std::move(state))
    , Descriptor_(descriptor)
{
}

TUnboxedValue TWasmUdfFunction::Run(
    const IValueBuilder* valueBuilder,
    const TUnboxedValuePod* args) const
{
    try {
        auto* queryHandle = GetCurrentQueryCompartment();
        Y_ENSURE(queryHandle && queryHandle->Compartment,
            "Query WASM compartment is not initialized");

        auto* compartment = queryHandle->Compartment.get();
        const TString exportName(PlainWasmExport(Descriptor_));
        const auto exportKey = MakeExportKey(State_->ModuleName, exportName);
        auto* exportIt = queryHandle->Exports.FindPtr(exportKey);
        Y_ENSURE(exportIt, "Missing WASM export binding for " << exportKey);

        compartment->SetTimeout(TDuration::Minutes(1));
        compartment->StartDeadlineTimer();
        TCurrentCompartmentGuard compartmentGuard(compartment);
        TWasmUdfInvocationContext context(compartment);
        TCurrentInvocationContextGuard invocationGuard(&context);

        TVector<TPreparedArg> preparedArgs;
        preparedArgs.reserve(Descriptor_.Args.size());
        TVector<uintptr_t> argOffsets;
        argOffsets.reserve(Descriptor_.Args.size());
        for (size_t i = 0; i < Descriptor_.Args.size(); ++i) {
            preparedArgs.push_back(PrepareArgFromUnboxed(compartment, args[i], Descriptor_.Args[i]));
            argOffsets.push_back(preparedArgs.back().Storage.Offset);
        }

        const auto resultOffset = compartment->AllocateBytes(sizeof(TUnversionedValue));
        auto resultGuard = TCopyGuard(compartment, resultOffset);
        StoreValue(compartment, resultOffset, MakeEmptyValue());

        InvokeUdfExport(
            compartment,
            *exportIt,
            exportName,
            std::bit_cast<uintptr_t>(&context),
            resultOffset,
            argOffsets);

        return ReadResultUnboxed(valueBuilder, compartment, resultOffset, Descriptor_.Result);
    } catch (const std::exception& ex) {
        WasmError(ex, TStringRef(Descriptor_.Name), valueBuilder);
    }
    return {};
}

TWasmSoModule::TWasmSoModule(TWasmCompartmentStatePtr state, TString moduleName)
    : State_(std::move(state))
    , ModuleName_(std::move(moduleName))
{
}

void TWasmSoModule::CleanupOnTerminate() const {
}

void TWasmSoModule::GetAllFunctions(IFunctionsSink& sink) const {
    if (State_->ModuleName != ModuleName_) {
        return;
    }
    for (const auto& name : State_->FunctionOrder) {
        const auto* descriptor = State_->Functions.FindPtr(name);
        auto entry = sink.Add(TStringRef(name));
        if (descriptor && descriptor->Binding == EWasmUdfBinding::TypeConfigCallable) {
            entry->SetTypeAwareness();
        }
    }
}

void TWasmSoModule::BuildFunctionTypeInfo(
    const TStringRef& name,
    TType* /*userType*/,
    const TStringRef& typeConfig,
    ui32 flags,
    IFunctionTypeInfoBuilder& builder) const
{
    try {
        if (State_->ModuleName != ModuleName_) {
            builder.SetError(TStringRef::Of("Unknown wasm UDF module"));
            return;
        }
        const TString functionName(name.Data(), name.Size());
        const auto* descriptor = State_->Functions.FindPtr(functionName);
        if (!descriptor) {
            builder.SetError(TStringRef::Of("Unknown wasm UDF function"));
            return;
        }

        const bool typesOnly = (flags & TFlags::TypesOnly) != 0;
        if (descriptor->Binding == EWasmUdfBinding::TypeConfigCallable) {
            TWasmConfiguredCallable::Register(
                builder,
                typesOnly,
                State_,
                *descriptor,
                TString(typeConfig.Data(), typeConfig.Size()));
        } else {
            TWasmUdfFunction::Register(builder, typesOnly, State_, *descriptor);
        }
    } catch (const std::exception&) {
        builder.SetError(CurrentExceptionMessage());
    }
}

TUniquePtr<IUdfModule> BuildWasmSoModule(TWasmCompartmentStatePtr state) {
    TString moduleName = state->ModuleName;
    return TUniquePtr<IUdfModule>(new TWasmSoModule(std::move(state), std::move(moduleName)));
}

} // namespace NKikimr::NUdfStore::NWasm
