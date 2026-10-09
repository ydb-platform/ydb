#pragma once

#include "registry.h"
#include "udf_function.h"

#include <yql/essentials/public/udf/udf_helpers.h>

namespace NKikimr::NUdfStore::NWasm {

//! TypeConfig → create(blob) → ui64 handle → call(handle, args…)
class TWasmConfiguredCallable: public NYql::NUdf::TBoxedValue {
public:
    static void Register(
        NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly,
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor,
        TString typeConfigBlob);

    ~TWasmConfiguredCallable() override;

private:
    TWasmConfiguredCallable(
        NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        TWasmCompartmentStatePtr state,
        const TWasmUdfDescriptor& descriptor,
        TString typeConfigBlob);

    NYql::NUdf::TUnboxedValue Run(
        const NYql::NUdf::IValueBuilder* valueBuilder,
        const NYql::NUdf::TUnboxedValuePod* args) const override;

    void EnsureObject(const NYql::NUdf::IValueBuilder* valueBuilder) const;

    //! Invokes DestroyExport for Handle_ when the current query compartment
    //! still matches CompartmentGeneration_. No-op if the compartment is gone
    //! (generation change / TLS cleared) — guest state dies with the compartment.
    void DestroyObjectIfAlive(const NYql::NUdf::IValueBuilder* valueBuilder = nullptr) const;

    std::unique_ptr<TWasmBridgeFunction> Create_;
    std::unique_ptr<TWasmBridgeFunction> Call_;
    std::unique_ptr<TWasmBridgeFunction> Destroy_;
    size_t ArgCount_ = 0;
    TString ConfigBlob_;
    TString Name_;

    mutable ui64 Handle_ = 0;
    mutable ui64 CompartmentGeneration_ = 0;
};

} // namespace NKikimr::NUdfStore::NWasm
