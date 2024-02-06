#include "yql_json_codec.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <util/string/join.h>

#include <cmath>

namespace NYql {
namespace NCommon {
namespace NJsonCodec {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NJson;

namespace {

constexpr i64 MAX_JS_SAFE_INTEGER = 9007199254740991; // 2^53 - 1; JavaScript Number.MAX_SAFE_INTEGER
constexpr i64 MIN_JS_SAFE_INTEGER = -9007199254740991; // -(2^53 - 1); JavaScript Number.MIN_SAFE_INTEGER

constexpr i8 DOUBLE_N_DIGITS = std::numeric_limits<double>::max_digits10;
constexpr i8 FLOAT_N_DIGITS = std::numeric_limits<float>::max_digits10;
constexpr EFloatToStringMode FLOAT_MODE = EFloatToStringMode::PREC_NDIGITS;
}

TJsonWriterConfig MakeJsonConfig() {
    TJsonWriterConfig config;
    config.DoubleNDigits = DOUBLE_N_DIGITS;
    config.FloatNDigits = FLOAT_N_DIGITS;
    config.FloatToStringMode = FLOAT_MODE;
    config.FormatOutput = false;
    config.SortKeys = false;
    config.ValidateUtf8 = false;
    config.DontEscapeStrings = false;
    config.WriteNanAsString = true;

    return config;
}

void WriteValueToJson(TJsonWriter& writer, const NKikimr::NUdf::TUnboxedValuePod& value,
                      NKikimr::NMiniKQL::TType* type, TValueConvertPolicy convertPolicy) {

    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
        writer.WriteNull();
        break;
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
        writer.OpenArray();
        writer.CloseArray();
        break;
    case TType::EKind::Data:
        {
            bool numberToStr = convertPolicy.Test(EValueConvertPolicy::NUMBER_AS_STRING);
            auto dataType = AS_TYPE(TDataType, type);
            switch (dataType->GetSchemeType()) {
            case NUdf::TDataType<bool>::Id: {
                auto boolValue = value.Get<bool>();
                if (convertPolicy.Test(EValueConvertPolicy::BOOL_AS_STRING)) {
                    writer.Write(boolValue ? "true" : "false");
                } else {
                    writer.Write(boolValue);
                }
                break;
            }
            case NUdf::TDataType<i32>::Id: {
                auto number = value.Get<i32>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<ui32>::Id: {
                auto number = value.Get<ui32>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<i64>::Id: {
                auto number = value.Get<i64>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else if (convertPolicy.Test(EValueConvertPolicy::UNSAFE_NUMBER_AS_STRING)) {
                    if (number > MAX_JS_SAFE_INTEGER || number < MIN_JS_SAFE_INTEGER) {
                        writer.Write(ToString(number));
                    } else {
                        writer.Write(number);
                    };
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<ui64>::Id: {
                auto number = value.Get<ui64>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else if (convertPolicy.Test(EValueConvertPolicy::UNSAFE_NUMBER_AS_STRING)) {
                    if (number > MAX_JS_SAFE_INTEGER) {
                        writer.Write(ToString(number));
                    } else {
                        writer.Write(number);
                    };
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<ui8>::Id: {
                auto number = value.Get<ui8>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<i8>::Id: {
                auto number = value.Get<i8>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<ui16>::Id: {
                auto number = value.Get<ui16>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<i16>::Id: {
                auto number = value.Get<i16>();
                if (numberToStr) {
                    writer.Write(ToString(number));
                } else {
                    writer.Write(number);
                }
                break;
            }
            case NUdf::TDataType<float>::Id: {
                auto floatValue = value.Get<float>();
                bool isUndetermined = std::isnan(floatValue) || std::isinf(floatValue);
                if (convertPolicy.Test(EValueConvertPolicy::DISALLOW_NaN) && isUndetermined) {
                    YQL_ENSURE(false, "NaN and Inf aren't allowed");
                }

                if (numberToStr) {
                    TString number = FloatToString(floatValue, FLOAT_MODE, FLOAT_N_DIGITS);
                    writer.Write(number);
                } else {
                    writer.Write(floatValue);
                }
                break;
            }
            case NUdf::TDataType<double>::Id: {
                auto doubleValue = value.Get<double>();
                bool isUndetermined = std::isnan(doubleValue) || std::isinf(doubleValue);
                if (convertPolicy.Test(EValueConvertPolicy::DISALLOW_NaN) && isUndetermined) {
                    YQL_ENSURE(false, "NaN and Inf aren't allowed");
                }

                if (numberToStr) {
                    TString number = FloatToString(doubleValue, FLOAT_MODE, DOUBLE_N_DIGITS);
                    writer.Write(number);
                } else {
                    writer.Write(doubleValue);
                }
                break;
            }
            case NUdf::TDataType<NUdf::TJson>::Id:
            case NUdf::TDataType<NUdf::TUtf8>::Id:
                writer.Write(value.AsStringRef());
                break;
            case NUdf::TDataType<char*>::Id: {
                TString encoded = Base64Encode(value.AsStringRef());
                writer.Write(encoded);
                break;
            }
            case NUdf::TDataType<NUdf::TDecimal>::Id: {
                const auto params = static_cast<TDataDecimalType*>(type)->GetParams();
                const auto str = NDecimal::ToString(value.GetInt128(), params.first, params.second);
                const auto size = str ? std::strlen(str) : 0;
                writer.Write(TStringBuf(str, size));
                break;
            }
            case NUdf::TDataType<NUdf::TUuid>::Id:
                writer.Write(value.AsStringRef());
                break;
            case NUdf::TDataType<NUdf::TYson>::Id:
            case NUdf::TDataType<NUdf::TDyNumber>::Id:
            case NUdf::TDataType<NUdf::TDate>::Id:
            case NUdf::TDataType<NUdf::TDatetime>::Id:
            case NUdf::TDataType<NUdf::TTimestamp>::Id:
            case NUdf::TDataType<NUdf::TInterval>::Id:
            case NUdf::TDataType<NUdf::TTzDate>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            case NUdf::TDataType<NUdf::TDate32>::Id:
            case NUdf::TDataType<NUdf::TDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            case NUdf::TDataType<NUdf::TInterval64>::Id:
            case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
                const NUdf::TUnboxedValue out(ValueToString(*dataType->GetDataSlot(), value));
                writer.Write(out.AsStringRef());
                break;
            }
            default:
                throw yexception() << "Unknown data type: " << dataType->GetSchemeType();
            }
        }
        break;
    case TType::EKind::Struct:
    {
        writer.OpenMap();
        auto structType = AS_TYPE(TStructType, type);
        for (ui32 i = 0, e = structType->GetMembersCount(); i < e; ++i) {
            writer.WriteKey(structType->GetMemberName(i));
            WriteValueToJson(writer, value.GetElement(i), structType->GetMemberType(i), convertPolicy);
        }
        writer.CloseMap();
        break;
    }
    case TType::EKind::List:
    {
        writer.OpenArray();
        auto listType = AS_TYPE(TListType, type);
        const auto it = value.GetListIterator();
        for (NUdf::TUnboxedValue item; it.Next(item);) {
            WriteValueToJson(writer, item, listType->GetItemType(), convertPolicy);
        }
        writer.CloseArray();
        break;
    }
    case TType::EKind::Optional:
    {
        writer.OpenArray();
        if (value) {
            auto optionalType = AS_TYPE(TOptionalType, type);
            WriteValueToJson(writer, value.GetOptionalValue(), optionalType->GetItemType(), convertPolicy);
        }
        writer.CloseArray();
        break;
    }
    case TType::EKind::Dict:
    {
        writer.OpenArray();
        auto dictType = AS_TYPE(TDictType, type);
        const auto it = value.GetDictIterator();
        // is Set<>
        if (dictType->GetPayloadType()->GetKind() == TType::EKind::Void) {
            for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                WriteValueToJson(writer, key, dictType->GetKeyType(), convertPolicy);
            }
        } else {
            for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                writer.OpenArray();
                WriteValueToJson(writer, key, dictType->GetKeyType(), convertPolicy);
                WriteValueToJson(writer, payload, dictType->GetPayloadType(), convertPolicy);
                writer.CloseArray();
            }
        }
        writer.CloseArray();
        break;
    }
    case TType::EKind::Tuple:
    {
        writer.OpenArray();
        auto tupleType = AS_TYPE(TTupleType, type);
        for (ui32 i = 0, e = tupleType->GetElementsCount(); i < e; ++i) {
            WriteValueToJson(writer, value.GetElement(i), tupleType->GetElementType(i), convertPolicy);
        }
        writer.CloseArray();
        break;
    }
    case TType::EKind::Variant:
    {
        writer.OpenArray();
        auto index = value.GetVariantIndex();
        auto underlyingType = AS_TYPE(TVariantType, type)->GetUnderlyingType();
        if (underlyingType->IsTuple()) {
            writer.Write(index);
            WriteValueToJson(writer, value.GetVariantItem(),
                             AS_TYPE(TTupleType, underlyingType)->GetElementType(index), convertPolicy);
        } else {
            auto structType = AS_TYPE(TStructType, underlyingType);
            writer.Write(structType->GetMemberName(index));
            WriteValueToJson(writer, value.GetVariantItem(), structType->GetMemberType(index), convertPolicy);
        }
        writer.CloseArray();
        break;
    }
    case TType::EKind::Tagged:
    {
        auto underlyingType = AS_TYPE(TTaggedType, type)->GetBaseType();
        WriteValueToJson(writer, value, underlyingType, convertPolicy);
        break;
    }
    default:
        YQL_ENSURE(false, "doesn't known how to convert type " << type->GetKindAsStr() << " to JSON");
    }
}

NKikimr::NUdf::TUnboxedValue ReadJsonValue(TJsonValue& json, NKikimr::NMiniKQL::TType* type,
    const NMiniKQL::THolderFactory& holderFactory)
{
    auto jsonType = json.GetType();
    switch (type->GetKind()) {
    case TType::EKind::Void:
    case TType::EKind::Null:
        YQL_ENSURE(json.IsNull(), "Unexpected json type (expected null value, but got type " << jsonType << ")");
        return NKikimr::NUdf::TUnboxedValuePod();
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
        YQL_ENSURE(json.IsArray(), "Unexpected json type (expected array, but got " << jsonType << ")");
        YQL_ENSURE(json.GetArray().size() == 0, "Expected empty array, but got array with " << json.GetArray().size() << " elements");
        return holderFactory.GetEmptyContainerLazy();
    case TType::EKind::Tuple:
    {
        YQL_ENSURE(json.IsArray(), "Unexpected json type (expected array, but got " << jsonType << ")");
        auto tupleType = AS_TYPE(TTupleType, type);
        auto array = json.GetArray();
        YQL_ENSURE(array.size() == tupleType->GetElementsCount(),
                   "Expected " << tupleType->GetElementsCount() << " elements in tuple, but got " << array.size());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue tuple = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
        for (ui32 i = 0, e = array.size(); i < e; i++) {
            items[i] = ReadJsonValue(array[i], tupleType->GetElementType(i), holderFactory);
        }
        return tuple;
    }
    case TType::EKind::Dict:
    {
        YQL_ENSURE(json.IsArray(), "Unexpected json type (expected array, but got " << jsonType << ")");
        auto dictType = AS_TYPE(TDictType, type);
        auto array = json.GetArray();
        auto builder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);
        // is Set<>
        if (dictType->GetPayloadType()->GetKind() == TType::EKind::Void) {
            for (ui32 i = 0, e = array.size(); i < e; i++) {
                auto value = ReadJsonValue(array[i], dictType->GetKeyType(), holderFactory);
                builder->Add(std::move(value), NKikimr::NUdf::TUnboxedValuePod());
            }
        } else {
            for (ui32 i = 0, e = array.size(); i < e; i++) {
                auto tuple = array[i];
                YQL_ENSURE(tuple.IsArray() && tuple.GetArray().size() == 2,
                           "Unexpected json type (expected array with 2 elements, but got " << tuple.GetType() << ")");

                auto key = ReadJsonValue(tuple[0], dictType->GetKeyType(), holderFactory);
                auto value = ReadJsonValue(tuple[1], dictType->GetPayloadType(), holderFactory);
                builder->Add(std::move(key), std::move(value));
            }
        }
        return builder->Build();
    }
    case TType::EKind::Variant:
    {
        YQL_ENSURE(json.IsArray(), "Unexpected json type (expected array, but got " << jsonType << ")");
        auto array = json.GetArray();
        YQL_ENSURE(array.size() == 2, "Expected array with 2 elements, but got array with " << array.size() << " elements");
        auto underlyingType = AS_TYPE(TVariantType, type)->GetUnderlyingType();
        NUdf::TUnboxedValuePod variant;
        if (underlyingType->IsTuple()) {
            auto indexType = json[0].GetType();
            YQL_ENSURE(indexType == EJsonValueType::JSON_INTEGER || indexType == EJsonValueType::JSON_UINTEGER,
                       "First element of variant has unexpected type(expected integer, but got " << indexType << ")");
            ui64 index = indexType == EJsonValueType::JSON_INTEGER ? json[0].GetInteger() : json[0].GetUInteger();
            auto tupleType = AS_TYPE(TTupleType, underlyingType);
            YQL_ENSURE(index < tupleType->GetElementsCount(), "Member with index " << index << " didn't find in Variant by tuple");
            auto value = ReadJsonValue(array[1], tupleType->GetElementType(index), holderFactory);
            variant = holderFactory.CreateVariantHolder(value.Release(), index);
        } else {
            auto structType = AS_TYPE(TStructType, underlyingType);
            YQL_ENSURE(json[0].IsString(), "First element of variant has unexpected type(expected string, but got " << json[0].GetType() << ")");
            auto index = structType->FindMemberIndex(json[0].GetString());
            YQL_ENSURE(index, "Member with name `" << json[0].GetString() << "` didn't find in Variant by struct");
            auto value = ReadJsonValue(array[1], structType->GetMemberType(*index), holderFactory);
            variant = holderFactory.CreateVariantHolder(value.Release(), *index);
        }
        return variant;
    }
    case TType::EKind::Tagged:
    {
        auto underlyingType = AS_TYPE(TTaggedType, type)->GetBaseType();
        return ReadJsonValue(json, underlyingType, holderFactory);
    }
    case TType::EKind::List:
    {
        YQL_ENSURE(json.IsArray(), "Unexpected json type (expected array, but got " << jsonType << ")");
        auto listType = AS_TYPE(TListType, type);
        TDefaultListRepresentation items;
        auto array = json.GetArray();
        for (ui32 i = 0, e = array.size(); i < e; i++) {
            items = items.Append(ReadJsonValue(array[i], listType->GetItemType(), holderFactory));
        }
        return holderFactory.CreateDirectListHolder(std::move(items));
    }
    case TType::EKind::Struct:
    {
        YQL_ENSURE(json.IsMap(), "Unexpected json type (expected map, but got " << jsonType << ")");
        auto structType = AS_TYPE(TStructType, type);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue structValue = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);
        auto jsonMap = json.GetMap();
        std::unordered_set<TString> unprocessed(jsonMap.size());
        for (auto const& map : jsonMap) {
            unprocessed.insert(map.first);
        }
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            const auto keyName = TString(structType->GetMemberName(i));
            if (jsonMap.contains(keyName)) {
                items[i] = ReadJsonValue(jsonMap[keyName], structType->GetMemberType(i), holderFactory);
            } else {
                YQL_ENSURE(structType->GetMemberType(i)->IsOptional(), "Absent non optional field " << keyName << " at struct");
                items[i] = NKikimr::NUdf::TUnboxedValue();
            }
            unprocessed.erase(keyName);
        }
        YQL_ENSURE(unprocessed.empty(), "Extra fields into json map detected (" << JoinSeq(',', unprocessed) << ")");
        return structValue;
    }
    case TType::EKind::Optional:
    {
        YQL_ENSURE(json.IsArray()
                    && json.GetArray().size() <= 1
                   , "Unexpected json type (expected array no more than one element, but got " << jsonType << ")");

        auto array = json.GetArray();
        if (array.empty()) {
            return NUdf::TUnboxedValuePod().MakeOptional();
        }
        auto optionalType = AS_TYPE(TOptionalType, type);
        auto value = ReadJsonValue(array.front(), optionalType->GetItemType(), holderFactory);
        return value.Release().MakeOptional();
    }
    case TType::EKind::Data:
        {
            auto dataType = AS_TYPE(TDataType, type);
            switch (dataType->GetSchemeType()) {
            case NUdf::TDataType<bool>::Id:
                YQL_ENSURE(json.IsBoolean(), "Unexpected json type (expected bool, but got " << jsonType << ")");
                return NUdf::TUnboxedValuePod(json.GetBoolean());

#define INTEGER_CONVERTOR(type, wideType) \
            case NUdf::TDataType<type>::Id: { \
                YQL_ENSURE(jsonType == EJsonValueType::JSON_INTEGER \
                    || jsonType == EJsonValueType::JSON_UINTEGER, \
                           "Unexpected json type (expected " << #type << ", but got " << jsonType << ")"); \
                wideType intValue = jsonType == EJsonValueType::JSON_INTEGER ? json.GetInteger() : json.GetUInteger(); \
                YQL_ENSURE(intValue >= std::numeric_limits<type>::min() \
                        && intValue <= std::numeric_limits<type>::max(), \
                        "Exceeded the range of acceptable values for " << #type); \
                return NUdf::TUnboxedValuePod(type(intValue)); \
            }

            INTEGER_CONVERTOR(ui8, ui64)
            INTEGER_CONVERTOR(ui16, ui64)
            INTEGER_CONVERTOR(ui32, ui64)
            INTEGER_CONVERTOR(ui64, ui64)

            INTEGER_CONVERTOR(i8, i64)
            INTEGER_CONVERTOR(i16, i64)
            INTEGER_CONVERTOR(i32, i64)
            INTEGER_CONVERTOR(i64, i64)

#undef INTEGER_CONVERTOR

            case NUdf::TDataType<float>::Id: {
                YQL_ENSURE(json.IsDouble() || json.IsInteger() || json.IsUInteger(),
                    "Unexpected json type (expected double or integer, but got " << jsonType << ")");
                double value = jsonType == EJsonValueType::JSON_DOUBLE
                        ? json.GetDouble()
                        : (jsonType == EJsonValueType::JSON_INTEGER ? double(json.GetInteger()) : double(json.GetUInteger()));
                YQL_ENSURE(value >= std::numeric_limits<float>::min() && value <= std::numeric_limits<float>::max(),
                            "Exceeded the range of acceptable values for float");
                return NUdf::TUnboxedValuePod(float(value));
            }
            case NUdf::TDataType<double>::Id: {
                YQL_ENSURE(json.IsDouble() || json.IsInteger() || json.IsUInteger(),
                   "Unexpected json type (expected double or integer, but got " << jsonType << ")");
                double value = jsonType == EJsonValueType::JSON_DOUBLE
                        ? json.GetDouble()
                        : (jsonType == EJsonValueType::JSON_INTEGER ? double(json.GetInteger()) : double(json.GetUInteger()));
                return NUdf::TUnboxedValuePod(value);
            }
            case NUdf::TDataType<NUdf::TUtf8>::Id:
            case NUdf::TDataType<char*>::Id: {
                YQL_ENSURE(json.IsString(), "Unexpected json type (expected string, but got " << jsonType << ")");
                auto value = json.GetString();
                return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(value)));
            }
            case NUdf::TDataType<NUdf::TDecimal>::Id: {
                YQL_ENSURE(json.IsString(), "Unexpected json type (expected string, but got " << jsonType << ")");
                const auto params = static_cast<TDataDecimalType*>(type)->GetParams();
                const auto value = NDecimal::FromString(json.GetString(), params.first, params.second);
                YQL_ENSURE(!NDecimal::IsError(value));
                return NUdf::TUnboxedValuePod(value);
            }
            case NUdf::TDataType<NUdf::TDate>::Id:
            case NUdf::TDataType<NUdf::TDatetime>::Id:
            case NUdf::TDataType<NUdf::TTimestamp>::Id:
            case NUdf::TDataType<NUdf::TInterval>::Id:
            case NUdf::TDataType<NUdf::TTzDate>::Id:
            case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            case NUdf::TDataType<NUdf::TDate32>::Id:
            case NUdf::TDataType<NUdf::TDatetime64>::Id:
            case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            case NUdf::TDataType<NUdf::TInterval64>::Id: {
                YQL_ENSURE(json.IsString(), "Unexpected json type (expected string, but got " << jsonType << ")");
                YQL_ENSURE(IsValidStringValue(*dataType->GetDataSlot(), json.GetString()), "Invalid date format (expected ISO-8601)");
                return ValueFromString(*dataType->GetDataSlot(), json.GetString());
            }
            default:
                YQL_ENSURE(false, "Can't convert from JSON (unsupported YQL type " << dataType->GetSchemeType() << ")");
            }
        }
        break;
    default:
        YQL_ENSURE(false, "Can't convert from JSON (unsupported YQL type " << type->GetKindAsStr() << ")");
    }

    return NKikimr::NUdf::TUnboxedValuePod();
}

}
}
}
