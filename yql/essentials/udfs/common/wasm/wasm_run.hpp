#pragma once

#include <yql/essentials/public/udf/udf_helpers.h>

#include "wasm_state.hpp"

namespace NWasm::NYQL {

class TRun: public TBoxedValue
{
public:
    static TStringRef Name();
    static TType* BuildFunctionType(IFunctionTypeInfoBuilder& builder);

    explicit TRun(TWasmRuntimeStatePtr state);

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    TWasmRuntimeStatePtr State_;
};

} // namespace NWasm::NYQL
