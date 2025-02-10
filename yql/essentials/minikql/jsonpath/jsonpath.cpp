#include "jsonpath.h"

#include <yql/essentials/minikql/jsonpath/parser/binary.h>
#include "executor.h"

using namespace NYql;
using namespace NYql::NUdf;
using namespace NJson;

namespace NYql::NJsonPath {

TResult ExecuteJsonPath(
    const TJsonPathPtr jsonPath,
    const TValue& json,
    const TVariablesMap& variables,
    const NUdf::IValueBuilder* valueBuilder) {
    TExecutor executor(jsonPath, {json}, variables, valueBuilder);
    return executor.Execute();
}

TVariablesMap DictToVariables(const NUdf::TUnboxedValue& dict) {
    TVariablesMap variables;
    TUnboxedValue key;
    TUnboxedValue payload;
    auto it = dict.GetDictIterator();
    while (it.NextPair(key, payload)) {
        variables[key.AsStringRef()] = TValue(payload);
    }
    return variables;
}

}
