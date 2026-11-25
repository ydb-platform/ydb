#include "functions.h"

#include <yt/yt/client/table_client/logical_type.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TTypeInferrerBase
{
public:
    explicit TTypeInferrerBase(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
    { }

    int GetIndex(
        TNormalizedContraints* constraints,
        std::unordered_map<TTypeParameter, int>* idToIndex,
        const TType& type,
        TStringBuf functionName) const
    {
        return Visit(type,
            [&] (TTypeParameter genericId) -> int {
                auto itIndex = idToIndex->find(genericId);
                if (itIndex != idToIndex->end()) {
                    return itIndex->second;
                } else {
                    int index = constraints->TypeConstraints.size();
                    auto it = TypeParameterConstraints_.find(genericId);
                    if (it == TypeParameterConstraints_.end()) {
                        constraints->TypeConstraints.push_back(TTypeSet({
                            EValueType::Null,
                            EValueType::Int64,
                            EValueType::Uint64,
                            EValueType::Double,
                            EValueType::Boolean,
                            EValueType::String,
                            EValueType::Any,
                        }));
                    } else {
                        constraints->TypeConstraints.push_back(TTypeSet(it->second.begin(), it->second.end()));
                    }
                    idToIndex->emplace(genericId, index);
                    return index;
                }
            },
            [&] (EValueType fixedType) -> int {
                int index = constraints->TypeConstraints.size();
                constraints->TypeConstraints.push_back(TTypeSet({fixedType}));
                return index;
            },
            [&] (const TUnionType& unionType) -> int {
                int index = constraints->TypeConstraints.size();
                constraints->TypeConstraints.push_back(TTypeSet(unionType.begin(), unionType.end()));
                return index;
            },
            [&] (const TLogicalTypePtr&) -> int {
                THROW_ERROR_EXCEPTION("Function %Qv is not supported in expression builder v1",
                    functionName);
            });
    }

protected:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
};

DECLARE_REFCOUNTED_CLASS(TFunctionTypeInferrer)

class TFunctionTypeInferrer
    : public ITypeInferrer
    , public TTypeInferrerBase
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : TTypeInferrerBase(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , RepeatedArgumentType_(std::move(repeatedArgumentType))
        , ResultType_(std::move(resultType))
    { }

    bool IsAggregate() const override
    {
        return false;
    }

    TNormalizedContraints GetNormalizedConstraints(TStringBuf functionName) const override
    {
        TNormalizedContraints constraints;
        std::unordered_map<TTypeParameter, int> idToIndex;

        for (const auto& argumentType : ArgumentTypes_) {
            constraints.FormalArguments.push_back(GetIndex(&constraints, &idToIndex, argumentType, functionName));
        }

        if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
            std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
        {
            constraints.RepeatedType = std::pair(
                GetIndex(&constraints, &idToIndex, RepeatedArgumentType_, functionName),
                std::get_if<TUnionType>(&RepeatedArgumentType_));
        }

        constraints.ReturnType = GetIndex(&constraints, &idToIndex, ResultType_, functionName);

        return constraints;
    }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        std::vector<TTypeId> argumentTypeIds;
        for (const auto& type : argumentTypes) {
            argumentTypeIds.push_back(typingCtx->GetTypeId(type));
        }

        auto signature = GetSignature(typingCtx, std::ssize(argumentTypes));

        return typingCtx->InferFunctionType(name, {signature}, argumentTypeIds);
    }

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx, int argumentCount) const
    {
        TTypingCtx::TFunctionSignature signature({});
        int nextGenericId = 0;

        auto registerConstraints = [&] (int id, const TUnionType& unionType) {
            if (id >= std::ssize(signature.Constraints)) {
                signature.Constraints.resize(id + 1);
            }
            for (auto type : unionType) {
                signature.Constraints[id].push_back(typingCtx->GetTypeId(type));
            }
        };

        Visit(ResultType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("Result type cannot be union");
            },
            [&] (const TLogicalTypePtr& logicalType) {
                signature.Types.push_back(typingCtx->GetTypeId(logicalType));
            });

        for (const auto& formalArgument : ArgumentTypes_) {
            Visit(formalArgument,
                [&] (TTypeParameter genericId) {
                    signature.Types.push_back(-(1 + genericId));
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                },
                [&] (const TUnionType& unionType) {
                    signature.Types.push_back(-(1 + nextGenericId));
                    registerConstraints(nextGenericId, unionType);
                    ++nextGenericId;
                },
                [&] (const TLogicalTypePtr& logicalType) {
                    signature.Types.push_back(typingCtx->GetTypeId(logicalType));
                });
        }

        if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
            std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
        {
            Visit(RepeatedArgumentType_,
                [&] (TTypeParameter genericId) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(-(1 + genericId));
                    }
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                    }
                },
                [&] (const TUnionType& unionType) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(-(1 + nextGenericId));
                        registerConstraints(nextGenericId, unionType);
                        ++nextGenericId;
                    }
                },
                [&] (const TLogicalTypePtr& logicalType) {
                    signature.Types.push_back(typingCtx->GetTypeId(logicalType));
                });
        }

        for (const auto& [parameterId, unionType] : TypeParameterConstraints_) {
            registerConstraints(parameterId, unionType);
        }

        return signature;
    }
};

DEFINE_REFCOUNTED_TYPE(TFunctionTypeInferrer)

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateFunctionTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
    TType repeatedArgumentType)
{
    return New<TFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::move(argumentTypes),
        std::move(repeatedArgumentType),
        std::move(resultType));
}

////////////////////////////////////////////////////////////////////////////////

class TAggregateFunctionTypeInferrer
    : public ITypeInferrer
    , public TTypeInferrerBase
{
public:
    TAggregateFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType stateType,
        TType resultType)
        : TTypeInferrerBase(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , RepeatedArgumentType_(std::move(repeatedArgType))
        , StateType_(std::move(stateType))
        , ResultType_(std::move(resultType))
    { }

    bool IsAggregate() const override
    {
        return true;
    }

    TNormalizedContraints GetNormalizedConstraints(TStringBuf functionName) const override
    {
        TNormalizedContraints constraints;
        std::unordered_map<TTypeParameter, int> idToIndex;

        for (const auto& argumentType : ArgumentTypes_) {
            constraints.FormalArguments.push_back(GetIndex(&constraints, &idToIndex, argumentType, functionName));
        }

        if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
            std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
        {
            constraints.RepeatedType = std::pair(
                GetIndex(&constraints, &idToIndex, RepeatedArgumentType_, functionName),
                std::get_if<TUnionType>(&RepeatedArgumentType_));
        }

        constraints.StateType = GetIndex(&constraints, &idToIndex, StateType_, functionName);
        constraints.ReturnType = GetIndex(&constraints, &idToIndex, ResultType_, functionName);

        return constraints;
    }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        std::vector<TTypeId> argumentTypeIds;
        for (const auto& type : argumentTypes) {
            argumentTypeIds.push_back(typingCtx->GetTypeId(GetWireType(type)));
        }

        auto signature = GetSignature(typingCtx, std::ssize(argumentTypes));

        // TODO(lukyan): Argument types and additional types (result, state)
        // Return two vectors?
        return typingCtx->InferFunctionType(name, {signature}, argumentTypeIds, 2);
    }

private:
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType StateType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx, int argumentCount) const
    {
        TTypingCtx::TFunctionSignature signature({});
        int nextGenericId = 0;

        auto registerConstraints = [&] (int id, const TUnionType& unionType) {
            if (id >= std::ssize(signature.Constraints)) {
                signature.Constraints.resize(id + 1);
            }
            for (auto type : unionType) {
                signature.Constraints[id].push_back(typingCtx->GetTypeId(type));
            }
        };

        Visit(ResultType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("Result type cannot be union");
            },
            [&] (const TLogicalTypePtr& logicalType) {
                signature.Types.push_back(typingCtx->GetTypeId(logicalType));
            });

        Visit(StateType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("State type cannot be union");
            },
            [&] (const TLogicalTypePtr& logicalType) {
                signature.Types.push_back(typingCtx->GetTypeId(logicalType));
            });

        for (const auto& formalArgument : ArgumentTypes_) {
            Visit(formalArgument,
                [&] (TTypeParameter genericId) {
                    signature.Types.push_back(-(1 + genericId));
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                },
                [&] (const TUnionType& unionType) {
                    signature.Types.push_back(-(1 + nextGenericId));
                    registerConstraints(nextGenericId, unionType);
                    ++nextGenericId;
                },
                [&] (const TLogicalTypePtr& logicalType) {
                    signature.Types.push_back(typingCtx->GetTypeId(logicalType));
                });
        }

        Visit(RepeatedArgumentType_,
            [&] (TTypeParameter genericId) {
                for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                    signature.Types.push_back(-(1 + genericId));
                }
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                if (fixedType == EValueType::Null) {
                    return;
                }
                for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                    signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                }
            },
            [&] (const TUnionType& unionType) {
                for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                    signature.Types.push_back(-(1 + nextGenericId));
                    registerConstraints(nextGenericId, unionType);
                    ++nextGenericId;
                }
            },
            [&] (const TLogicalTypePtr& logicalType) {
                signature.Types.push_back(typingCtx->GetTypeId(logicalType));
            });

        for (const auto& [parameterId, unionType] : TypeParameterConstraints_) {
            registerConstraints(parameterId, unionType);
        }

        return signature;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints)
{
    return New<TAggregateFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::move(argumentTypes),
        std::move(repeatedArgType),
        std::move(stateType),
        std::move(resultType));
}

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    TType argumentType,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints)
{
    return New<TAggregateFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::vector<TType>{std::move(argumentType)},
        EValueType::Null,
        std::move(stateType),
        std::move(resultType));
}

////////////////////////////////////////////////////////////////////////////////

class TArrayAggTypeInferrer
    : public TAggregateFunctionTypeInferrer
{
public:
    TArrayAggTypeInferrer()
        : TAggregateFunctionTypeInferrer(
            /*typeParameterConstraints*/ {},
            {
                TUnionType{
                    EValueType::String,
                    EValueType::Uint64,
                    EValueType::Int64,
                    EValueType::Double,
                    EValueType::Boolean,
                    EValueType::Any,
                },
                EValueType::Boolean,
            },
            EValueType::Null,
            EValueType::String,
            EValueType::Any)
    { }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(argumentTypes.size() == 2,
            "Expected two arguments for %Qv function, got %v",
            name,
            argumentTypes.size());

        return {
            typingCtx->GetTypeId(ListLogicalType(argumentTypes[0])),
            typingCtx->GetTypeId(EValueType::String),
            typingCtx->GetTypeId(argumentTypes[0]),
            typingCtx->GetTypeId(EValueType::Boolean),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDummyTypeInferrer
    : public ITypeInferrer
{
public:
    TDummyTypeInferrer(std::string name, bool aggregate, bool supportedInV1, bool supportedInV2)
        : Name_(std::move(name))
        , Aggregate_(aggregate)
        , SupportedInV1_(supportedInV1)
        , SupportedInV2_(supportedInV2)
    { }

    bool IsAggregate() const override
    {
        return Aggregate_;
    }

    [[noreturn]] TNormalizedContraints GetNormalizedConstraints(TStringBuf /*functionName*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(SupportedInV1_, "Function %Qv is not supported in expression builder v1",
            Name_);
        YT_ABORT();
    }

    [[noreturn]] std::vector<TTypeId> InferTypes(
        TTypingCtx* /*typingCtx*/,
        TRange<TLogicalTypePtr> /*argumentTypes*/,
        TStringBuf /*name*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(SupportedInV2_, "Function %Qv is not supported in expression builder v2",
            Name_);
        YT_ABORT();
    }

private:
    const std::string Name_;
    const bool Aggregate_;
    const bool SupportedInV1_;
    const bool SupportedInV2_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateArrayAggTypeInferrer()
{
    return New<TArrayAggTypeInferrer>();
}

ITypeInferrerPtr CreateDummyTypeInferrer(
    std::string name,
    bool aggregate,
    bool supportedInV1,
    bool supportedInV2)
{
    return New<TDummyTypeInferrer>(std::move(name), aggregate, supportedInV1, supportedInV2);
}

////////////////////////////////////////////////////////////////////////////////

const ITypeInferrerPtr& TTypeInferrerMap::GetFunction(const std::string& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Undefined function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const std::string& name)
{
    return
        name == "int64" ||
        name == "uint64" ||
        name == "double" ||
        name == "boolean" ||
        name == "to_any" ||
        name == "cast_operator";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
