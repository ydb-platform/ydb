#include "udf_configured_callable.h"

#include "compartment_manager.h"
#include "udf_function.h"

#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <exception>
#include <utility>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;

void TWasmConfiguredCallable::Register(
    IFunctionTypeInfoBuilder& builder,
    bool typesOnly,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor,
    TString typeConfigBlob)
{
    builder.Returns(BuildTypeFromWasmTypeNode(builder, *descriptor.ResultType));
    auto args = builder.Args(descriptor.ArgTypes.size());
    for (const auto& arg : descriptor.ArgTypes) {
        args->Add(BuildTypeFromWasmTypeNode(builder, *arg));
    }
    args->Done();
    if (!typesOnly) {
        builder.Implementation(new TWasmConfiguredCallable(builder, std::move(state), descriptor, std::move(typeConfigBlob)));
    }
}

TWasmConfiguredCallable::TWasmConfiguredCallable(
    IFunctionTypeInfoBuilder& builder,
    TWasmCompartmentStatePtr state,
    const TWasmUdfDescriptor& descriptor,
    TString typeConfigBlob)
    : ConfigBlob_(std::move(typeConfigBlob))
    , Name_(descriptor.Name)
{
    TWasmUdfDescriptor create;
    create.Name = create.ExportName = descriptor.CreateExport;
    create.ArgTypes = {MakeLeafTypeNode(EUdfValueType::String)};
    create.ResultType = MakeLeafTypeNode(EUdfValueType::Uint64);
    Create_ = TWasmBridgeFunction::Create(builder, state, create);

    auto call = descriptor;
    call.Binding = EWasmUdfBinding::Plain;
    call.ExportName = descriptor.CallExport;
    call.ArgTypes.insert(call.ArgTypes.begin(), MakeLeafTypeNode(EUdfValueType::Uint64));
    Call_ = TWasmBridgeFunction::Create(builder, state, call);
    ArgCount_ = descriptor.ArgTypes.size();

    if (!descriptor.DestroyExport.empty()) {
        TWasmUdfDescriptor destroy;
        destroy.Name = destroy.ExportName = descriptor.DestroyExport;
        destroy.ArgTypes = {MakeLeafTypeNode(EUdfValueType::Uint64)};
        destroy.ResultType = MakeLeafTypeNode(EUdfValueType::Null);
        Destroy_ = TWasmBridgeFunction::Create(builder, state, destroy);
    }
}

TWasmConfiguredCallable::~TWasmConfiguredCallable() {
    try {
        DestroyObjectIfAlive();
    } catch (...) {
        // Best-effort cleanup; guest memory also dies with its compartment.
    }
}

void TWasmConfiguredCallable::DestroyObjectIfAlive(const IValueBuilder* valueBuilder) const {
    const ui64 handle = std::exchange(Handle_, 0);
    const ui64 generation = std::exchange(CompartmentGeneration_, 0);
    auto* query = GetCurrentQueryCompartment();
    if (!handle || !Destroy_ || !query || !query->Compartment || query->Generation != generation) {
        return;
    }
    const TUnboxedValuePod arg(handle);
    Destroy_->Invoke(valueBuilder, &arg);
}

void TWasmConfiguredCallable::EnsureObject(const IValueBuilder* valueBuilder) const {
    auto* query = GetCurrentQueryCompartment();
    Y_ENSURE(query && query->Compartment && query->Generation, "Query WASM compartment is not initialized");
    if (Handle_ && CompartmentGeneration_ == query->Generation) {
        return;
    }
    DestroyObjectIfAlive(valueBuilder);
    const auto config = valueBuilder->NewString(TStringRef(ConfigBlob_));
    const auto created = Create_->Invoke(valueBuilder, &config);
    const ui64 handle = created.Get<ui64>();
    Y_ENSURE(handle, Name_ << ": create export returned zero object handle");
    Handle_ = handle;
    CompartmentGeneration_ = query->Generation;
}

TUnboxedValue TWasmConfiguredCallable::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
    try {
        EnsureObject(valueBuilder);
        TVector<TUnboxedValuePod> callArgs;
        callArgs.reserve(ArgCount_ + 1);
        callArgs.push_back(TUnboxedValuePod(Handle_));
        for (size_t i = 0; i < ArgCount_; ++i) {
            callArgs.push_back(args[i]);
        }
        return Call_->Invoke(valueBuilder, callArgs.data());
    } catch (...) {
        const auto original = std::current_exception();
        try {
            DestroyObjectIfAlive(valueBuilder);
        } catch (...) {
            // Preserve the original error, including termination classification.
        }
        try {
            std::rethrow_exception(original);
        } catch (const NKikimr::NMiniKQL::TTerminateException&) {
            throw;
        } catch (const NYql::TErrorException&) {
            throw;
        } catch (const std::exception& ex) {
            UdfTerminate((TStringBuilder() << Name_ << "(); ex: " << ex.what()).c_str());
        }
    }
    return {};
}

} // namespace NKikimr::NUdfStore::NWasm
