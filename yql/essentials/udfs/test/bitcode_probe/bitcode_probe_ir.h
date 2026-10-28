#pragma once

#include <yql/essentials/public/udf/udf_value.h>

#include <util/system/compiler.h>

namespace NYql::NUdf {

// Arithmetic and nothing else on purpose: anything that asserts drags the guts of Y_ABORT_UNLESS
// into the bitcode, and the JIT would then fail to resolve them for a reason of its own.
extern "C" UDF_ALWAYS_INLINE void TwiceIR(
    const IBoxedValue* /*pThis*/,
    TUnboxedValuePod* result,
    const IValueBuilder* /*valueBuilder*/,
    const TUnboxedValuePod* args)
{
    *result = TUnboxedValuePod(args[0].Get<double>() * 2.0);
}

} // namespace NYql::NUdf
