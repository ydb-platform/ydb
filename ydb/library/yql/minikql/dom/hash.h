#pragma once

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>

namespace NYql::NDom {

NUdf::THashType HashDom(const NUdf::TUnboxedValuePod value);

bool EquateDoms(const NUdf::TUnboxedValuePod lhs, const NUdf::TUnboxedValuePod rhs);

}

