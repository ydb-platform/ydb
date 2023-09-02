#include "functions.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

int TFunctionTypeInferrer::GetNormalizedConstraints(
    std::vector<TTypeSet>* typeConstraints,
    std::vector<int>* formalArguments,
    std::optional<std::pair<int, bool>>* repeatedType) const
{
    std::unordered_map<TTypeParameter, int> idToIndex;

    auto getIndex = [&] (const TType& type) -> int {
        return Visit(type,
            [&] (TTypeParameter genericId) -> int {
                auto itIndex = idToIndex.find(genericId);
                if (itIndex != idToIndex.end()) {
                    return itIndex->second;
                } else {
                    int index = typeConstraints->size();
                    auto it = TypeParameterConstraints_.find(genericId);
                    if (it == TypeParameterConstraints_.end()) {
                        typeConstraints->push_back(TTypeSet({
                            EValueType::Null,
                            EValueType::Int64,
                            EValueType::Uint64,
                            EValueType::Double,
                            EValueType::Boolean,
                            EValueType::String,
                            EValueType::Any}));
                    } else {
                        typeConstraints->push_back(TTypeSet(it->second.begin(), it->second.end()));
                    }
                    idToIndex.emplace(genericId, index);
                    return index;
                }
            },
            [&] (EValueType fixedType) -> int {
                int index = typeConstraints->size();
                typeConstraints->push_back(TTypeSet({fixedType}));
                return index;
            },
            [&] (const TUnionType& unionType) -> int {
                int index = typeConstraints->size();
                typeConstraints->push_back(TTypeSet(unionType.begin(), unionType.end()));
                return index;
            });
    };

    for (const auto& argumentType : ArgumentTypes_) {
        formalArguments->push_back(getIndex(argumentType));
    }

    if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
        std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
    {
        *repeatedType = std::make_pair(
            getIndex(RepeatedArgumentType_),
            std::get_if<TUnionType>(&RepeatedArgumentType_));
    }

    return getIndex(ResultType_);
}

void TAggregateTypeInferrer::GetNormalizedConstraints(
    TTypeSet* constraint,
    std::optional<EValueType>* stateType,
    std::optional<EValueType>* resultType,
    TStringBuf name) const
{
    if (TypeParameterConstraints_.size() > 1) {
        THROW_ERROR_EXCEPTION("Too many constraints for aggregate function");
    }

    auto setType = [&] (const TType& targetType, bool allowGeneric) -> std::optional<EValueType> {
        if (auto* fixedType = std::get_if<EValueType>(&targetType)) {
            return *fixedType;
        }
        if (allowGeneric) {
            if (auto* typeId = std::get_if<TTypeParameter>(&targetType)) {
                auto found = TypeParameterConstraints_.find(*typeId);
                if (found != TypeParameterConstraints_.end()) {
                    return std::nullopt;
                }
            }
        }
        THROW_ERROR_EXCEPTION("Invalid type constraints for aggregate function %Qv", name);
    };

    Visit(ArgumentType_,
        [&] (const TUnionType& unionType) {
            *constraint = TTypeSet(unionType.begin(), unionType.end());
            *resultType = setType(ResultType_, false);
            *stateType = setType(StateType_, false);
        },
        [&] (EValueType fixedType) {
            *constraint = TTypeSet({fixedType});
            *resultType = setType(ResultType_, false);
            *stateType = setType(StateType_, false);
        },
        [&] (TTypeParameter typeId) {
            auto found = TypeParameterConstraints_.find(typeId);
            if (found == TypeParameterConstraints_.end()) {
                THROW_ERROR_EXCEPTION("Invalid type constraints for aggregate function %Qv", name);
            }

            *constraint = TTypeSet(found->second.begin(), found->second.end());
            *resultType = setType(ResultType_, true);
            *stateType = setType(StateType_, true);
        });
}

std::pair<int, int> TAggregateFunctionTypeInferrer::GetNormalizedConstraints(
    std::vector<TTypeSet>* typeConstraints,
    std::vector<int>* argumentConstraintIndexes) const
{
    std::unordered_map<TTypeParameter, int> idToIndex;

    auto getIndex = [&] (const TType& type) -> int {
        return Visit(type,
            [&] (EValueType fixedType) -> int {
                typeConstraints->push_back(TTypeSet({fixedType}));
                return typeConstraints->size() - 1;
            },
            [&] (TTypeParameter genericId) -> int {
                auto itIndex = idToIndex.find(genericId);
                if (itIndex != idToIndex.end()) {
                    return itIndex->second;
                } else {
                    int index = typeConstraints->size();
                    auto it = TypeParameterConstraints_.find(genericId);
                    if (it == TypeParameterConstraints_.end()) {
                        typeConstraints->push_back(TTypeSet({
                            EValueType::Null,
                            EValueType::Int64,
                            EValueType::Uint64,
                            EValueType::Double,
                            EValueType::Boolean,
                            EValueType::String,
                            EValueType::Any}));
                    } else {
                        typeConstraints->push_back(TTypeSet(it->second.begin(), it->second.end()));
                    }
                    idToIndex.emplace(genericId, index);
                    return index;
                }
            },
            [&] (const TUnionType& unionType) -> int {
                typeConstraints->push_back(TTypeSet(unionType.begin(), unionType.end()));
                return typeConstraints->size() - 1;
            });
    };

    for (const auto& argumentType : ArgumentTypes_) {
        argumentConstraintIndexes->push_back(getIndex(argumentType));
    }

    return std::make_pair(getIndex(StateType_), getIndex(ResultType_));
}


////////////////////////////////////////////////////////////////////////////////

const ITypeInferrerPtr& TTypeInferrerMap::GetFunction(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Undefined function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const TString& name)
{
    return name == "int64" || name == "uint64" || name == "double";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
