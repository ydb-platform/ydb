#pragma once
#include <yql/essentials/public/udf/udf_value.h>

namespace NYql {

template<typename ValueType>
NUdf::TUnboxedValuePod ScalarValueToPod(const ValueType value);

}
