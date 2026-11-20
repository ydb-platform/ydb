#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions_common.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionRegistryBuilder
{
    virtual ~IFunctionRegistryBuilder() = default;

    virtual void RegisterFunction(
        const std::string& functionName,
        const std::string& symbolName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool useFunctionContext = false) = 0;

    virtual void RegisterFunction(
        const std::string& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention) = 0;

    virtual void RegisterFunction(
        const std::string& functionName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile) = 0;

    virtual void RegisterAggregate(
        const std::string& aggregateName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType,
        TType stateType,
        TType repeatedArgType,
        TStringBuf implementationFile,
        bool isFirst = false) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
