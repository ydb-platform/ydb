#pragma once

#include "registry.h"
#include "bridge_types.h"
#include "bridge_node_table.h"

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NYdb::NWasm {
struct IWebAssemblyCompartment;
} // namespace NYdb::NWasm

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;

//! Build the exact manifest type, with a depth bound for hand-built descriptors.
TType* BuildTypeFromWasmTypeNode(
    IFunctionTypeInfoBuilder& builder,
    const TWasmTypeNode& node,
    ui32 depth = 0);

//! Declared result reduced to what a returned handle can be checked against.
//! Optionality is transparent on both sides of the bridge and says nothing
//! about the payload, so the family is taken from under the Optional layers:
//! a declared Optional<List<...>> still may not be answered with a scalar.
struct TDeclaredResultShape {
    //! Family of the payload; Null when the declared type tells us nothing.
    EBridgeKindFamily Family = EBridgeKindFamily::Null;
    //! Declared type accepts a null, so the guest may return one.
    bool Optional = false;
    //! Alternatives of the declared Variant; zero when the payload is not one.
    //! A returned Variant carries an index MiniKQL indexes the underlying type
    //! with, without a range check of its own.
    ui32 VariantAlternatives = 0;
    //! Tag of the declared Resource; empty when the payload is not one. Two
    //! Resources differ by nothing else, and what is behind one is a void*
    //! the next UDF casts to whatever its own tag implies.
    TString ResourceTag;

    //! `kind` is the kind of the returned node, `payload` the kind of the
    //! value inside it when that node is an Optional the guest built. An
    //! Optional over an unknown payload passes only for a declared container:
    //! MiniKQL represents an Optional container as the container itself, so
    //! there is nothing left to compare, while an optional scalar or string
    //! always arrives with its payload named.
    bool Accepts(
        EBridgeValueKind kind,
        std::optional<EBridgeValueKind> payload = std::nullopt) const;

    //! Same question asked of the family a value will present to MiniKQL,
    //! which is what a node knows once its type is taken into account.
    //! Nothing means the node names no family at all.
    bool AcceptsFamily(std::optional<EBridgeKindFamily> family) const;
};

TDeclaredResultShape DeclaredResultShape(const TType* type, const ITypeInfoHelper* helper);

class TWasmBridgeFunction: public TBoxedValue {
public:
    static std::unique_ptr<TWasmBridgeFunction> Create(
        IFunctionTypeInfoBuilder& builder,
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor);

    //! Shared bridge invocation for plain calls and object lifecycle callbacks.
    TUnboxedValue Invoke(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const;

    static void Register(
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly,
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor);

private:
    TWasmBridgeFunction(
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor,
        TVector<TType*> argTypes,
        TType* resultType,
        ITypeInfoHelper::TPtr typeInfoHelper);

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    //! Nothing downstream re-reads the declared type, so a guest that returns
    //! a handle of the wrong shape hands MiniKQL a value it will read as the
    //! declared one. Compare what can be compared cheaply: the family of the
    //! returned node against the payload family of ResultType_.
    void EnsureResultFamily(const TWasmBridgeNodeTable::TNode& node) const;

    TWasmCompartmentStatePtr State_;
    TWasmUdfDescriptor Descriptor_;
    TVector<TType*> ArgTypes_;
    TType* ResultType_ = nullptr;
    ITypeInfoHelper::TPtr TypeInfoHelper_;
    TDeclaredResultShape ResultShape_;
};

class TWasmSoModule: public IUdfModule {
public:
    TWasmSoModule(TWasmCompartmentStatePtr state, TString moduleName);

    void CleanupOnTerminate() const final;

    void GetAllFunctions(IFunctionsSink& sink) const final;

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final;

private:
    TWasmCompartmentStatePtr State_;
    TString ModuleName_;
};

TUniquePtr<IUdfModule> BuildWasmSoModule(TWasmCompartmentStatePtr state);

} // namespace NKikimr::NUdfStore::NWasm
