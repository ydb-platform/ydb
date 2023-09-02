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
        "length",
        std::vector<TType>{EValueType::String},
        EValueType::Int64,
        "length",
        ECallingConvention::Simple);

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
            EValueType::String
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
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Double,
            EValueType::String,
            EValueType::Any
        },
        EValueType::Any,
        "make_map");

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
        "regex_full_match",
        "regex_full_match",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_partial_match",
        "regex_partial_match",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_first",
        "regex_replace_first",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_all",
        "regex_replace_all",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_extract",
        "regex_extract",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_escape",
        "regex_escape",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    const TTypeParameter typeParameter = 0;
    auto anyConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraints[typeParameter] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
        EValueType::Any};

    builder->RegisterAggregate(
        "first",
        anyConstraints,
        typeParameter,
        typeParameter,
        typeParameter,
        "first",
        ECallingConvention::UnversionedValue,
        true);

    auto xdeltaConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    xdeltaConstraints[typeParameter] = std::vector<EValueType>{
        EValueType::Null,
        EValueType::String};
    builder->RegisterAggregate(
        "xdelta",
        xdeltaConstraints,
        typeParameter,
        typeParameter,
        typeParameter,
        "xdelta",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "avg",
        std::unordered_map<TTypeParameter, TUnionType>(),
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        "avg",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "cardinality",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<EValueType>{
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean},
        EValueType::Uint64,
        EValueType::String,
        "hyperloglog",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "format_timestamp",
        std::vector<TType>{EValueType::Int64, EValueType::String},
        EValueType::String,
        "dates",
        ECallingConvention::Simple);

    std::vector<TString> timestampFloorFunctions = {
        "timestamp_floor_hour",
        "timestamp_floor_day",
        "timestamp_floor_week",
        "timestamp_floor_month",
        "timestamp_floor_year"};

    for (const auto& name : timestampFloorFunctions) {
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            "dates",
            ECallingConvention::Simple);
    }

    builder->RegisterFunction(
        "format_guid",
        std::vector<TType>{EValueType::Uint64, EValueType::Uint64},
        EValueType::String,
        "format_guid",
        ECallingConvention::Simple);

    std::vector<std::pair<TString, EValueType>> ypathGetFunctions = {
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
        {"get_any", EValueType::Any}};

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
        std::vector<TType>{
            TUnionType{
                EValueType::String,
                EValueType::Uint64,
                EValueType::Int64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::Any,
                EValueType::Composite}},
        EValueType::Any,
        "to_any",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "list_contains",
        std::vector<TType>{
            EValueType::Any,
            TUnionType{
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
            }},
        EValueType::Boolean,
        "list_contains",
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
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::Any, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "has_permissions",
        ECallingConvention::UnversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
