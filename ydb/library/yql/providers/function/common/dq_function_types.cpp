#include "dq_function_types.h"

namespace NYql::NDqFunction {

TDqFunctionResolver::TDqFunctionResolver() {}

TDqFunctionDescription TDqFunctionResolver::AddFunction(const TDqFunctionType& type, const TString& functionName, const TString& connection) {
    auto function = TDqFunctionDescription{
        .Type = type,
        .FunctionName = functionName,
        .Connection = connection
    };
    return *(FunctionsDescription.emplace(function).first);
}

std::vector<TDqFunctionDescription> TDqFunctionResolver::FunctionsToResolve() {
    return {FunctionsDescription.begin(), FunctionsDescription.end()};
}

}