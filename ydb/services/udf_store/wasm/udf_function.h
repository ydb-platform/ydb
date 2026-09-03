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
TType* BuildTypeFromWasmTypeNode(
    IFunctionTypeInfoBuilder& builder,
    const TWasmTypeNode& node,
    bool topLevel = true);

//! Map a type node to bridge value/node kinds for registration.
void BridgeKindsFromTypeNode(
    const TWasmTypeNode& node,
    EBridgeNodeKind& outNodeKind,
    EBridgeValueKind& outValueKind);

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

    TWasmCompartmentStatePtr State_;
    TWasmUdfDescriptor Descriptor_;
    TVector<TType*> ArgTypes_;
    TType* ResultType_ = nullptr;
    ITypeInfoHelper::TPtr TypeInfoHelper_;
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
