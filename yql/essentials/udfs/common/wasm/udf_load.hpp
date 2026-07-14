#pragma once

#include "wasm_udf_registry.hpp"

#include <yql/essentials/public/udf/udf_helpers.h>

namespace NWasm::NYQL {

class TLoadUdfs: public TBoxedValue
{
public:
    static TStringRef Name();
    static void Register(IFunctionTypeInfoBuilder& builder, bool typesOnly);

private:
    explicit TLoadUdfs(ui32 describeIdx, ui32 runIdx);

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    ui32 DescribeIdx_;
    ui32 RunIdx_;
};

} // namespace NWasm::NYQL
