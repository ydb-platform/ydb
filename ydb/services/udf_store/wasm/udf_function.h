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

NYql::NUdf::TUnboxedValue ReadResultUnboxed(
    const NYql::NUdf::IValueBuilder* valueBuilder,
    NYdb::NWasm::IWebAssemblyCompartment* compartment,
    uintptr_t resultOffset,
    EUdfValueType expectedType);

//! Build MiniKQL/UDF TType* from a recursive manifest type node. A leaf at the
//! top level of an argument / result becomes Optional<data> (the shape
//! unversioned_value always had); nested leaves are built verbatim.
//! `depth` mirrors MaxManifestTypeDepth in the parser so a hand-built tree
//! cannot blow the stack either.
TType* BuildTypeFromWasmTypeNode(
    IFunctionTypeInfoBuilder& builder,
    const TWasmTypeNode& node,
    bool topLevel = true,
    ui32 depth = 0);

//! Map a type node to bridge value/node kinds for registration.
void BridgeKindsFromTypeNode(
    const TWasmTypeNode& node,
    EBridgeNodeKind& outNodeKind,
    EBridgeValueKind& outValueKind);

//! Declared result reduced to what a returned handle can be checked against.
//! Optionality is transparent on both sides of the bridge and says nothing
//! about the payload, so the family is taken from under the Optional layers:
//! a declared Optional<List<...>> still may not be answered with a scalar.
struct TDeclaredResultShape {
    //! Family of the payload; Null when the declared type tells us nothing.
    EBridgeKindFamily Family = EBridgeKindFamily::Null;
    //! Declared type accepts a null, so the guest may return one.
    bool Optional = false;

    //! `kind` is the kind of the returned node, `payload` the kind of the
    //! value inside it when that node is an Optional the guest built. An
    //! Optional over an unknown payload passes: MiniKQL represents an
    //! Optional container as the container itself, so there is nothing left
    //! to compare.
    bool Accepts(
        EBridgeValueKind kind,
        std::optional<EBridgeValueKind> payload = std::nullopt) const;
};

TDeclaredResultShape DeclaredResultShape(const TType* type, const ITypeInfoHelper* helper);

class TWasmUdfFunction: public TBoxedValue {
public:
    static TType* BuildYqlType(IFunctionTypeInfoBuilder& builder, EUdfValueType type);

    static TType* BuildFunctionType(
        IFunctionTypeInfoBuilder& builder,
        const TWasmUdfDescriptor& descriptor);

    static void Register(
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly,
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor);

private:
    TWasmUdfFunction(TWasmCompartmentStatePtr state, const TWasmUdfDescriptor& descriptor);

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    TWasmCompartmentStatePtr State_;
    TWasmUdfDescriptor Descriptor_;
};

//! Bridge calling-convention UDF: args/result are ui64 handles into the
//! per-query TWasmBridgeNodeTable (no TUnversionedValue marshalling).
class TWasmBridgeFunction: public TBoxedValue {
public:
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
