#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>

namespace NKikimr {
namespace NMiniKQL {

NUdf::TUnboxedValuePod AppendString(const NUdf::TUnboxedValuePod value, const NUdf::TStringRef ref);
NUdf::TUnboxedValuePod PrependString(const NUdf::TStringRef ref, const NUdf::TUnboxedValuePod value);
NUdf::TUnboxedValuePod ConcatStrings(const NUdf::TUnboxedValuePod first, const NUdf::TUnboxedValuePod second);
NUdf::TUnboxedValuePod SubString(const NUdf::TUnboxedValuePod value, ui32 offset, ui32 size);
NUdf::TUnboxedValuePod MakeString(const NUdf::TStringRef ref);
NUdf::TUnboxedValuePod MakeStringNotFilled(ui32 size, ui32 pad = 0U);

}
}
