#pragma once

#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

namespace NYql::NDom {

NUdf::TUnboxedValuePod MakeDom(const NUdf::ITypeInfoHelper* typeHelper, const NUdf::TType* shape, const NUdf::TUnboxedValuePod value, const NUdf::IValueBuilder* valueBuilder);

}
