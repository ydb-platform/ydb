#pragma once

#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NMiniKQL {

NUdf::TUnboxedValuePod AppendString(NUdf::TUnboxedValuePod value, NUdf::TStringRef ref);
NUdf::TUnboxedValuePod PrependString(NUdf::TStringRef ref, NUdf::TUnboxedValuePod value);
NUdf::TUnboxedValuePod ConcatStrings(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod second);
NUdf::TUnboxedValuePod SubString(NUdf::TUnboxedValuePod value, ui32 offset, ui32 size);
NUdf::TUnboxedValuePod MakeString(NUdf::TStringRef ref);
NUdf::TUnboxedValuePod MakeStringNotFilled(ui32 size, ui32 pad = 0U);

} // namespace NKikimr::NMiniKQL
