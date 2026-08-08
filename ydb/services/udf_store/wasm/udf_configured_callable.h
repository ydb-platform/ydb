#pragma once

#include "registry.h"

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

private:
    TWasmConfiguredCallable(
        TWasmCompartmentStatePtr state,
        TWasmUdfDescriptor descriptor,
        TString typeConfigBlob);

    NYql::NUdf::TUnboxedValue Run(
        const NYql::NUdf::IValueBuilder* valueBuilder,
        const NYql::NUdf::TUnboxedValuePod* args) const override;

    void EnsureObject(NYql::NUdf::TStringRef functionNameForErrors) const;

    TWasmCompartmentStatePtr State_;
    TWasmUdfDescriptor Descriptor_;
    TString ConfigBlob_;

    mutable ui64 Handle_ = 0;
    mutable ui64 CompartmentGeneration_ = 0;
    mutable uintptr_t PinnedBlobOffset_ = 0;
    mutable ui32 PinnedBlobLength_ = 0;
};

} // namespace NKikimr::NUdfStore::NWasm
