#include "builtin_function_registry.h"

#include "functions.h"

#include <library/cpp/resource/resource.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(IFunctionRegistryBuilder* builder)
{
    builder->RegisterFunction(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        "is_substr",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        "lower",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "to_valid_utf8",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        "to_valid_utf8",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "is_valid_utf8",
        std::vector<TType>{EValueType::String},
        EValueType::Boolean,
        "to_valid_utf8",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "length",
        std::vector<TType>{EValueType::String},
        EValueType::Int64,
        "length",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "split",
        std::vector<TType>{EValueType::String, EValueType::String},
        OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
        "split",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "yson_length",
        std::vector<TType>{EValueType::Any},
        EValueType::Int64,
        "yson_length",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "concat",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::String,
        "concat",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        "sleep",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "farm_hash",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::String,
        },
        EValueType::Uint64,
        "farm_hash");

    builder->RegisterFunction(
        "bigb_hash",
        std::vector<TType>{EValueType::String},
        EValueType::Uint64,
        "bigb_hash",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "make_map",
        {},
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Double,
            EValueType::String,
            EValueType::Any,
        },
        EValueType::Any,
        "make_map");

    builder->RegisterFunction(
        "make_list",
        {},
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Double,
            EValueType::String,
            EValueType::Any,
        },
        EValueType::Any,
        "make_list");

    builder->RegisterFunction(
        "make_entity",
        std::vector<TType>{},
        EValueType::Any,
        "make_entity",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "numeric_to_string",
        std::vector<TType>{
            TUnionType{
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
            }},
        EValueType::String,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_int64",
        std::vector<TType>{EValueType::String},
        EValueType::Int64,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_uint64",
        std::vector<TType>{EValueType::String},
        EValueType::Uint64,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_double",
        std::vector<TType>{EValueType::String},
        EValueType::Double,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "is_finite",
        std::vector<TType>{EValueType::Double},
        EValueType::Boolean,
        "is_finite",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "regex_full_match",
        "regex_full_match",
        {},
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_partial_match",
        "regex_partial_match",
        {},
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_first",
        "regex_replace_first",
        {},
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_all",
        "regex_replace_all",
        {},
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_extract",
        "regex_extract",
        {},
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_escape",
        "regex_escape",
        {},
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "make_ngrams",
        "make_ngrams",
        {},
        std::vector<TType>{EValueType::String, EValueType::Int64},
        EValueType::Null,
        OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
        "ngrams",
        ECallingConvention::UnversionedValue);

    const TTypeParameter typeParameter = 0;
    auto anyConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraints[typeParameter] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
        EValueType::Any
    };

    builder->RegisterAggregate(
        "first",
        anyConstraints,
        {typeParameter},
        typeParameter,
        typeParameter,
        /*repeatedArgType*/ EValueType::Null,
        "first",
        true);

    auto xdeltaConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    xdeltaConstraints[typeParameter] = {
        EValueType::Null,
        EValueType::String,
    };
    builder->RegisterAggregate(
        "xdelta",
        xdeltaConstraints,
        {typeParameter},
        typeParameter,
        typeParameter,
        /*repeatedArgType*/ EValueType::Null,
        "xdelta");

    for (auto merge : {false, true}) {
        for (auto state : {false, true}) {
            auto argType = merge
                ? TType(EValueType::String)
                : TUnionType{
                    EValueType::String,
                    EValueType::Uint64,
                    EValueType::Int64,
                    EValueType::Double,
                    EValueType::Boolean,
                };
            auto returnType = state ? EValueType::String : EValueType::Uint64;

            // COMPAT(dtorilov): Remove after 25.4.
            // COMPAT BEGIN {

            builder->RegisterAggregate(
                Format("cardinality%v%v", merge ? "_merge" : "", state ? "_state" : ""),
                {},
                {argType},
                returnType,
                EValueType::String,
                /*repeatedArgType*/ EValueType::Null,
                "hyperloglog");

            // } COMPAT END

            for (int i = 7; i <= 14; ++i) {
                builder->RegisterAggregate(
                    Format("hll_%v%v%v", i, merge ? "_merge" : "", state ? "_state" : ""),
                    {},
                    {argType},
                    returnType,
                    EValueType::String,
                    /*repeatedArgType*/EValueType::Null,
                    "hyperloglog");
            }
        }
    }

    for (auto merge : {false, true}) {
        for (auto state : {false, true}) {
            builder->RegisterAggregate(
                Format("uniq%v%v", merge ? "_merge" : "", state ? "_state" : ""),
                {},
                /*argumentTypes*/ merge ? std::vector<TType>{EValueType::String} : std::vector<TType>{},
                /*returnType*/ state ? EValueType::String : EValueType::Uint64,
                /*stateType*/ EValueType::String,
                /*repeatedArgType*/ merge ? TType(EValueType::Null) : TUnionType{
                    EValueType::String,
                    EValueType::Uint64,
                    EValueType::Int64,
                    EValueType::Double,
                    EValueType::Boolean,
                    EValueType::Any,
                },
                "uniq");
        }
    }

    {
        const TTypeParameter summable = 0;
        auto sumConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        sumConstraints[summable] = {
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
        };

        builder->RegisterFunction(
            "running_sum",
            "running_sum",
            sumConstraints,
            {summable},
            /*repeatedArgType*/ EValueType::Null,
            summable,
            "running",
            ECallingConvention::UnversionedValue,
            /*useFunctionContext*/ true);
    }

    {
        const TTypeParameter anyValue = 0;
        auto uniqConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        uniqConstraints[anyValue] = {
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::Any,
        };

        builder->RegisterFunction(
            "running_uniq",
            "running_uniq",
            uniqConstraints,
            {anyValue},
            /*repeatedArgType*/ EValueType::Null,
            EValueType::Uint64,
            "running",
            ECallingConvention::UnversionedValue,
            /*useFunctionContext*/ true);
    }

    {
        const TTypeParameter differencable = 0;
        auto differenceConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        differenceConstraints[differencable] = {
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
        };

        builder->RegisterFunction(
            "running_difference",
            "running_difference",
            differenceConstraints,
            {differencable},
            /*repeatedArgType*/ EValueType::Null,
            differencable,
            "running",
            ECallingConvention::UnversionedValue,
            /*useFunctionContext*/ true);
    }

    builder->RegisterAggregate(
        "array_agg",
        {},
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
        EValueType::Any,
        EValueType::String,
        /*repeatedArgType*/ EValueType::Null,
        "array_agg");

    builder->RegisterAggregate(
        "dict_sum",
        {},
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        /*repeatedArgType*/ EValueType::Null,
        "dict_sum");

    for (const auto& name : std::vector<std::string>{
        "format_timestamp",
        "format_timestamp_localtime",
    }) {
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64, EValueType::String},
            EValueType::String,
            "dates",
            ECallingConvention::Simple);
    }

    for (const auto& name : std::vector<std::string>{
        "timestamp_floor_hour",
        "timestamp_floor_day",
        "timestamp_floor_week",
        "timestamp_floor_month",
        "timestamp_floor_quarter",
        "timestamp_floor_year",
        "timestamp_floor_hour_localtime",
        "timestamp_floor_day_localtime",
        "timestamp_floor_week_localtime",
        "timestamp_floor_month_localtime",
        "timestamp_floor_quarter_localtime",
        "timestamp_floor_year_localtime",
    }) {
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            "dates",
            ECallingConvention::Simple);
    }

    for (const auto& name : std::vector<std::string>{
        "timestamp_floor_hour_tz",
        "timestamp_floor_day_tz",
        "timestamp_floor_week_tz",
        "timestamp_floor_month_tz",
        "timestamp_floor_quarter_tz",
        "timestamp_floor_year_tz",
    }) {
        builder->RegisterFunction(
            name,
            name,
            {},
            std::vector<TType>{EValueType::Int64, EValueType::String},
            EValueType::Null,
            EValueType::Int64,
            "dates",
            ECallingConvention::Simple,
            /*useFunctionContext*/ true);
    }

    builder->RegisterFunction(
        "format_timestamp_tz",
        "format_timestamp_tz",
        {},
        std::vector<TType>{EValueType::Int64, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "dates",
        ECallingConvention::Simple,
        /*useFunctionContext*/ true);

    builder->RegisterFunction(
        "format_guid",
        std::vector<TType>{EValueType::Uint64, EValueType::Uint64},
        EValueType::String,
        "format_guid",
        ECallingConvention::Simple);

    std::vector<std::pair<std::string, EValueType>> ypathGetFunctions = {
        {"try_get_int64", EValueType::Int64},
        {"get_int64", EValueType::Int64},
        {"try_get_uint64", EValueType::Uint64},
        {"get_uint64", EValueType::Uint64},
        {"try_get_double", EValueType::Double},
        {"get_double", EValueType::Double},
        {"try_get_boolean", EValueType::Boolean},
        {"get_boolean", EValueType::Boolean},
        {"try_get_string", EValueType::String},
        {"get_string", EValueType::String},
        {"try_get_any", EValueType::Any},
        {"get_any", EValueType::Any},
    };

    for (const auto& fns : ypathGetFunctions) {
        auto&& name = fns.first;
        auto&& type = fns.second;
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Any, EValueType::String},
            type,
            "ypath_get",
            ECallingConvention::UnversionedValue);
    }

    builder->RegisterFunction(
        "to_any",
        std::vector<TType>{0},
        EValueType::Any,
        "to_any",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "yson_string_to_any",
        std::vector<TType>{EValueType::String},
        EValueType::Any,
        "yson_string_to_any",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "list_contains",
        std::vector<TType>{
            EValueType::Any,
            TUnionType{
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
            },
        },
        EValueType::Boolean,
        "list_contains",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "list_has_intersection",
        std::vector<TType>{
            EValueType::Any,
            EValueType::Any,
        },
        EValueType::Boolean,
        "list_has_intersection",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "any_to_yson_string",
        std::vector<TType>{EValueType::Any},
        EValueType::String,
        "any_to_yson_string",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "_yt_has_permissions",
        "has_permissions",
        {},
        std::vector<TType>{EValueType::Any, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "has_permissions",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "_yt_stored_replica_set",
        {},
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        /*repeatedArgType*/ EValueType::Null,
        "yt_stored_replica_set");

    builder->RegisterAggregate(
        "_inferrum_kv_cache_replica_set",
        {},
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        /*repeatedArgType*/ EValueType::Null,
        "inferrum_kv_cache_replica_set");

    builder->RegisterAggregate(
        "_yt_last_seen_replica_set",
        {},
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        /*repeatedArgType*/ EValueType::Null,
        "yt_last_seen_replica_set");

    const TTypeParameter typeParameterGreatest = 0;
    auto anyConstraintsGreatest = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraintsGreatest[typeParameterGreatest] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    builder->RegisterFunction(
        "greatest",
        anyConstraintsGreatest,
        {typeParameterGreatest},
        typeParameterGreatest,
        typeParameterGreatest,
        "greatest");

    const TTypeParameter typeParameterAbs = 0;
    auto anyConstraintsAbs = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraintsAbs[typeParameterAbs] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
    };

    builder->RegisterFunction(
        "abs",
        "math_abs",
        anyConstraintsAbs,
        {typeParameterAbs},
        EValueType::Null,
        typeParameterAbs,
        "math_abs",
        ECallingConvention::UnversionedValue,
        false);

    struct TMathFunction {
        std::string Name;
        std::vector<TType> ArgumentTypes;
        EValueType ResultType;
    };

    std::vector<TMathFunction> mathFunctions = {
        {"acos", {EValueType::Double}, EValueType::Double},
        {"asin", {EValueType::Double}, EValueType::Double},
        {"ceil", {EValueType::Double}, EValueType::Int64},
        {"cbrt", {EValueType::Double}, EValueType::Double},
        {"cos", {EValueType::Double}, EValueType::Double},
        {"cot", {EValueType::Double}, EValueType::Double},
        {"degrees", {EValueType::Double}, EValueType::Double},
        {"even", {EValueType::Double}, EValueType::Int64},
        {"exp", {EValueType::Double}, EValueType::Double},
        {"floor", {EValueType::Double}, EValueType::Int64},
        {"gamma", {EValueType::Double}, EValueType::Double},
        {"is_inf", {EValueType::Double}, EValueType::Boolean},
        {"lgamma", {EValueType::Double}, EValueType::Double},
        {"ln", {EValueType::Double}, EValueType::Double},
        {"log", {EValueType::Double}, EValueType::Double},
        {"log10", {EValueType::Double}, EValueType::Double},
        {"log2", {EValueType::Double}, EValueType::Double},
        {"radians", {EValueType::Double}, EValueType::Double},
        {"sign", {EValueType::Double}, EValueType::Int64},
        {"signbit", {EValueType::Double}, EValueType::Boolean},
        {"sin", {EValueType::Double}, EValueType::Double},
        {"sqrt", {EValueType::Double}, EValueType::Double},
        {"tan", {EValueType::Double}, EValueType::Double},
        {"trunc", {EValueType::Double}, EValueType::Int64},
        {"bit_count", {EValueType::Uint64}, EValueType::Int64},
        {"atan2", {EValueType::Double, EValueType::Double}, EValueType::Double},
        {"factorial", {EValueType::Uint64}, EValueType::Uint64},
        {"gcd", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
        {"lcm", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
        {"pow", {EValueType::Double, EValueType::Double}, EValueType::Double},
        {"round", {EValueType::Double}, EValueType::Int64},
        {"xor", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
    };

    for (const auto& [name, argumentType, resultType] : mathFunctions) {
        builder->RegisterFunction(
            name,
            "math_" + name,
            {},
            argumentType,
            EValueType::Null,
            resultType,
            "math",
            ECallingConvention::Simple,
            false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
