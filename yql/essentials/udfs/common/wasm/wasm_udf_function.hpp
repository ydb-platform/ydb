#pragma once

#include "wasm_directory_state.hpp"
#include "wasm_udf_registry.hpp"

#include <yql/essentials/public/udf/udf_helpers.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

//! Typed YQL UDF backed by a wasm `.so` export.
//! Lives inside a per-directory compartment (TWasmDirectoryState) and dispatches a single
//! YT-ABI function call (`void(TExpressionContext*, TUnversionedValue* result, TUnversionedValue* args...)`).
class TWasmUdfFunction: public TBoxedValue
{
public:
    static TType* BuildYqlType(IFunctionTypeInfoBuilder& builder, EUdfValueType type);

    //! Build YQL Callable type `Optional<R>(Optional<A0>, Optional<A1>, ...)` from the descriptor.
    static TType* BuildFunctionType(
        IFunctionTypeInfoBuilder& builder,
        const TWasmUdfDescriptor& descriptor);

    static void Register(
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly,
        TWasmDirectoryStatePtr state,
        const TWasmUdfDescriptor& descriptor);

private:
    TWasmUdfFunction(TWasmDirectoryStatePtr state, const TWasmUdfDescriptor& descriptor);

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    TWasmDirectoryStatePtr State_;
    TWasmUdfDescriptor Descriptor_;
};

//! IUdfModule that exposes the functions of a single `.so` from the shared directory state.
class TWasmSoModule: public IUdfModule
{
public:
    TWasmSoModule(TWasmDirectoryStatePtr state, TString moduleName);

    void CleanupOnTerminate() const final;

    void GetAllFunctions(IFunctionsSink& sink) const final;

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final;

private:
    TWasmDirectoryStatePtr State_;
    TString ModuleName_;
};

} // namespace NWasm::NYQL
