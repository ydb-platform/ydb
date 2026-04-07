#pragma once

#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

namespace NYql::NDom {

NUdf::THashType HashDom(NUdf::TUnboxedValuePod value);

bool EquateDoms(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs);

} // namespace NYql::NDom
