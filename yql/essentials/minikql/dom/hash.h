#pragma once

#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

namespace NYql::NDom {

NUdf::THashType HashDom(const NUdf::TUnboxedValuePod value);

bool EquateDoms(const NUdf::TUnboxedValuePod lhs, const NUdf::TUnboxedValuePod rhs);

}

