#pragma once

#include "public.h"

#include "key_trie.h"
#include "constraints.h"
#include "functions_common.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class ITypeInferrer
    : public virtual TRefCounted
{
public:
    template <class TDerived>
    const TDerived* As() const
    {
        return dynamic_cast<const TDerived*>(this);
    }

    template <class TDerived>
    TDerived* As()
    {
        return dynamic_cast<TDerived*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(ITypeInferrer)

////////////////////////////////////////////////////////////////////////////////

class TFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , RepeatedArgumentType_(repeatedArgumentType)
        , ResultType_(resultType)
    { }

    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(
            std::move(typeParameterConstraints),
            std::move(argumentTypes),
            EValueType::Null,
            resultType)
    { }

    TFunctionTypeInferrer(
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(
            std::unordered_map<TTypeParameter, TUnionType>(),
            std::move(argumentTypes),
            resultType)
    { }

    int GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* formalArguments,
        std::optional<std::pair<int, bool>>* repeatedType) const;

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType ResultType_;
};

class TAggregateTypeInferrer
    : public ITypeInferrer
{
public:
    TAggregateTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        TType argumentType,
        TType resultType,
        TType stateType)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
        , ArgumentType_(argumentType)
        , ResultType_(resultType)
        , StateType_(stateType)
    { }

    void GetNormalizedConstraints(
        TTypeSet* constraint,
        std::optional<EValueType>* stateType,
        std::optional<EValueType>* resultType,
        TStringBuf name) const;

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const TType ArgumentType_;
    const TType ResultType_;
    const TType StateType_;
};

class TAggregateFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TAggregateFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType stateType,
        TType resultType)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , StateType_(stateType)
        , ResultType_(resultType)
    { }

    std::pair<int, int> GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* argumentConstraintIndexes) const;

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType StateType_;
    const TType ResultType_;
};

////////////////////////////////////////////////////////////////////////////////

using TRangeExtractor = std::function<TKeyTriePtr(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)>;

using TConstraintExtractor = std::function<TConstraintRef(
    TConstraintsHolder* constraints,
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)>;

////////////////////////////////////////////////////////////////////////////////

struct TTypeInferrerMap
    : public TRefCounted
    , public std::unordered_map<TString, ITypeInferrerPtr>
{
    const ITypeInferrerPtr& GetFunction(const TString& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TTypeInferrerMap)

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const TString& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
