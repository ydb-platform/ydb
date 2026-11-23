#pragma once

#include "public.h"

#include "functions_common.h"
#include "typing.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TNormalizedContraints
{
    std::vector<TTypeSet> TypeConstraints;
    std::vector<int> FormalArguments;
    std::optional<std::pair<int, bool>> RepeatedType;
    int StateType = -1;
    int ReturnType;
};

struct ITypeInferrer
    : public virtual TRefCounted
{
    virtual bool IsAggregate() const = 0;

    virtual TNormalizedContraints GetNormalizedConstraints(TStringBuf functionName) const = 0;

    virtual std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITypeInferrer)

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateFunctionTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints = {},
    TType repeatedArgumentType = EValueType::Null);

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints = {});

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    TType argumentType,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints = {});

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateArrayAggTypeInferrer();
ITypeInferrerPtr CreateDummyTypeInferrer(
    std::string name,
    bool aggregate,
    bool supportedInV1,
    bool supportedInV2);

////////////////////////////////////////////////////////////////////////////////

struct TTypeInferrerMap
    : public TRefCounted
    , public std::unordered_map<std::string, ITypeInferrerPtr>
{
    const ITypeInferrerPtr& GetFunction(const std::string& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TTypeInferrerMap)

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
