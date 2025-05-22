#pragma once

#include "executor.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

namespace NYql::NJsonPath {

TVariablesMap DictToVariables(const NUdf::TUnboxedValue& dict);

TResult ExecuteJsonPath(
    const TJsonPathPtr jsonPath,
    const TValue& json,
    const TVariablesMap& variables,
    const NUdf::IValueBuilder* valueBuilder);

}
