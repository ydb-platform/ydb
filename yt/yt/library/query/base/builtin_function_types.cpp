#include "builtin_function_types.h"

#include "functions_builder.h"
#include "functions.h"

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTypeInferrerFunctionRegistryBuilder
    : public IFunctionRegistryBuilder
{
public:
    explicit TTypeInferrerFunctionRegistryBuilder(const TTypeInferrerMapPtr& typeInferrers)
        : TypeInferrers_(typeInferrers)
    { }

    void RegisterFunction(
        const std::string& functionName,
        const std::string& /*symbolName*/,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/,
        bool /*useFunctionContext*/) override
    {
        TypeInferrers_->emplace(functionName, CreateFunctionTypeInferrer(
            resultType,
            std::move(argumentTypes),
            std::move(typeParameterConstraints),
            repeatedArgType));
    }

    void RegisterFunction(
        const std::string& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/) override
    {
        TypeInferrers_->emplace(functionName, CreateFunctionTypeInferrer(
            resultType,
            std::move(argumentTypes)));
    }

    void RegisterFunction(
        const std::string& functionName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/) override
    {
        TypeInferrers_->emplace(functionName, CreateFunctionTypeInferrer(
            resultType,
            std::move(argumentTypes),
            std::move(typeParameterConstraints),
            repeatedArgType));
    }

    void RegisterAggregate(
        const std::string& aggregateName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType,
        TType stateType,
        TType repeatedArgType,
        TStringBuf /*implementationFile*/,
        bool /*isFirst*/) override
    {
        TypeInferrers_->emplace(aggregateName, CreateAggregateTypeInferrer(
            resultType,
            std::move(argumentTypes),
            repeatedArgType,
            stateType,
            typeParameterConstraints));
    }

private:
    const TTypeInferrerMapPtr TypeInferrers_;
};

std::unique_ptr<IFunctionRegistryBuilder> CreateTypeInferrerFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers)
{
    return std::make_unique<TTypeInferrerFunctionRegistryBuilder>(typeInferrers);
}

////////////////////////////////////////////////////////////////////////////////

TConstTypeInferrerMapPtr CreateBuiltinTypeInferrers()
{
    auto result = New<TTypeInferrerMap>();

    const TTypeParameter primitive = 0;

    result->emplace("if", CreateFunctionTypeInferrer(
        primitive,
        std::vector<TType>{EValueType::Boolean, primitive, primitive}));

    result->emplace("is_prefix", CreateFunctionTypeInferrer(
        EValueType::Boolean,
        std::vector<TType>{EValueType::String, EValueType::String}));

    result->emplace("is_null", CreateFunctionTypeInferrer(
        EValueType::Boolean,
        std::vector<TType>{primitive}));

    result->emplace("is_nan", CreateFunctionTypeInferrer(
        EValueType::Boolean,
        std::vector<TType>{EValueType::Double}));

    result->emplace("is_finite", CreateFunctionTypeInferrer(
        EValueType::Boolean,
        std::vector<TType>{EValueType::Double}));

    const TTypeParameter castable = 0;

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Any,
        };

        result->emplace("int64", CreateFunctionTypeInferrer(
            EValueType::Int64,
            std::vector<TType>{castable},
            castConstraints));

        result->emplace("uint64", CreateFunctionTypeInferrer(
            EValueType::Uint64,
            std::vector<TType>{castable},
            castConstraints));

        result->emplace("double", CreateFunctionTypeInferrer(
            EValueType::Double,
            std::vector<TType>{castable},
            castConstraints));
    }

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Any,
        };

        result->emplace("boolean", CreateFunctionTypeInferrer(
            EValueType::Boolean,
            std::vector<TType>{castable},
            castConstraints));
    }

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::String,
            EValueType::Any,
        };

        result->emplace("string", CreateFunctionTypeInferrer(
            EValueType::String,
            std::vector<TType>{castable},
            castConstraints));
    }

    result->emplace("if_null", CreateFunctionTypeInferrer(
        primitive,
        std::vector<TType>{primitive, primitive}));

    const TTypeParameter nullable = 0;

    std::unordered_map<TTypeParameter, TUnionType> coalesceConstraints;
    coalesceConstraints[nullable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Composite,
        EValueType::Any,
    };
    result->emplace("coalesce", CreateFunctionTypeInferrer(
        nullable,
        /*argumentTypes*/ {},
        coalesceConstraints,
        nullable));

    const TTypeParameter summable = 0;
    auto sumConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    sumConstraints[summable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
    };

    result->emplace("sum", CreateAggregateTypeInferrer(
        summable,
        summable,
        summable,
        sumConstraints));

    const TTypeParameter comparable = 0;
    auto minMaxConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    minMaxConstraints[comparable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    for (const auto& name : {"min", "max"}) {
        result->emplace(name, CreateAggregateTypeInferrer(
            comparable,
            comparable,
            comparable,
            minMaxConstraints));
    }

    auto argMinMaxConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    argMinMaxConstraints[0] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    for (const auto& name : {"argmin", "argmax"}) {
        result->emplace(name, CreateAggregateTypeInferrer(
            /*resultType*/ 1,
            /*argumentTypes*/ {1, 0},
            /*repeatedArgType*/ EValueType::Null,
            /*stateType*/ EValueType::String,
            argMinMaxConstraints));
    }

    result->emplace("avg", CreateAggregateTypeInferrer(
        EValueType::Double,
        summable,
        EValueType::String,
        sumConstraints));

    result->emplace("array_agg", CreateArrayAggTypeInferrer());

    result->emplace("cast_operator", CreateDummyTypeInferrer(
        "cast_operator",
        /*aggregate*/ false,
        /*supportedInV1*/ false,
        /*supportedInV2*/ true));

    TTypeInferrerFunctionRegistryBuilder builder{result.Get()};
    RegisterBuiltinFunctions(&builder);

    return result;
}

const TConstTypeInferrerMapPtr GetBuiltinTypeInferrers()
{
    static const auto builtinTypeInferrers = CreateBuiltinTypeInferrers();
    return builtinTypeInferrers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
