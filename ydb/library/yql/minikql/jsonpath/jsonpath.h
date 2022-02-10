#pragma once

#include "executor.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NJsonPath {

const TAstNodePtr ParseJsonPathAst(const TStringBuf path, TIssues& issues, size_t maxParseErrors);

const TJsonPathPtr PackBinaryJsonPath(const TAstNodePtr ast, TIssues& issues);

const TJsonPathPtr ParseJsonPath(const TStringBuf path, TIssues& issues, size_t maxParseErrors);

TVariablesMap DictToVariables(const NUdf::TUnboxedValue& dict);

TResult ExecuteJsonPath(
    const TJsonPathPtr jsonPath,
    const TValue& json,
    const TVariablesMap& variables,
    const NUdf::IValueBuilder* valueBuilder);

}
