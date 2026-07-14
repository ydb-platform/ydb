#pragma once

#include <yql/essentials/public/udf/udf_helpers.h>

#include "wasm_state.hpp"

namespace NWasm::NYQL {

class TInit: public TBoxedValue
{
public:
    static TStringRef Name();
    static void Register(IFunctionTypeInfoBuilder& builder, bool typesOnly);

private:
    explicit TInit(ui32 describeIdx, ui32 runIdx);

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    ui32 DescribeIdx_;
    ui32 RunIdx_;
};

} // namespace NWasm::NYQL
