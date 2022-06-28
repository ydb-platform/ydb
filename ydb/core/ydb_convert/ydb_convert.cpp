#include "ydb_convert.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>

#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/utils/utf8.h>

namespace NKikimr {

template<typename TOut>
Y_FORCE_INLINE void ConvertMiniKQLTupleTypeToYdbType(const NKikimrMiniKQL::TTupleType& protoTupleType, TOut& output) {
    const ui32 elementsCount = static_cast<ui32>(protoTupleType.ElementSize());
    for (ui32 elementIdx = 0; elementIdx < elementsCount; ++elementIdx) {
        const NKikimrMiniKQL::TType& elementProtoType = protoTupleType.GetElement(elementIdx);
        ConvertMiniKQLTypeToYdbType(elementProtoType, *output.add_elements());
    }
}

template<typename TOut>
Y_FORCE_INLINE void ConvertMiniKQLStructTypeToYdbType(const NKikimrMiniKQL::TStructType& protoStructType, TOut& output) {
    const ui32 membersCount = static_cast<ui32>(protoStructType.MemberSize());
    for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
        const NKikimrMiniKQL::TMember& protoMember = protoStructType.GetMember(memberNum);
        auto newMember = output.add_members();
        newMember->set_name(protoMember.GetName());
        ConvertMiniKQLTypeToYdbType(protoMember.GetType(), *newMember->mutable_type());
    }
}

template<typename TOut>
Y_FORCE_INLINE void ConvertYdbTupleTypeToMiniKQLType(const Ydb::TupleType& protoTupleType, TOut& output) {
    const ui32 elementsCount = static_cast<ui32>(protoTupleType.elementsSize());
    for (ui32 elementIdx = 0; elementIdx < elementsCount; ++elementIdx) {
        const Ydb::Type& elementProtoType = protoTupleType.elements(elementIdx);
        ConvertYdbTypeToMiniKQLType(elementProtoType, *output.AddElement());
    }
}

template<typename TOut>
Y_FORCE_INLINE void ConvertYdbStructTypeToMiniKQLType(const Ydb::StructType& protoStructType, TOut& output) {
    const ui32 membersCount = static_cast<ui32>(protoStructType.membersSize());
    for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
        const Ydb::StructMember& protoMember = protoStructType.members(memberNum);
        auto newMember = output.AddMember();
        newMember->SetName(protoMember.name());
        ConvertYdbTypeToMiniKQLType(protoMember.type(), *newMember->MutableType());
    }
}

void ConvertMiniKQLTypeToYdbType(const NKikimrMiniKQL::TType& input, Ydb::Type& output) {
    switch (input.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void: {
            output.set_void_type(::google::protobuf::NULL_VALUE);
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Null: {
            output.set_null_type(::google::protobuf::NULL_VALUE);
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Data: {
            const NKikimrMiniKQL::TDataType& protoData = input.GetData();
            NUdf::TDataTypeId schemeType = protoData.GetScheme();
            Y_VERIFY(NUdf::FindDataSlot(schemeType), "unknown type id: %d", (int) schemeType);
            if (schemeType == NYql::NProto::TypeIds::Decimal) {
                auto typeParams = output.mutable_decimal_type();
                typeParams->set_precision(protoData.GetDecimalParams().GetPrecision());
                typeParams->set_scale(protoData.GetDecimalParams().GetScale());
            } else {
                NMiniKQL::ExportPrimitiveTypeToProto(schemeType, output);
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Optional: {
            const NKikimrMiniKQL::TOptionalType& protoOptionalType = input.GetOptional();
            ConvertMiniKQLTypeToYdbType(protoOptionalType.GetItem(), *output.mutable_optional_type()->mutable_item());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::List: {
            const NKikimrMiniKQL::TListType& protoListType = input.GetList();
            ConvertMiniKQLTypeToYdbType(protoListType.GetItem(), *output.mutable_list_type()->mutable_item());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Tuple: {
            const NKikimrMiniKQL::TTupleType& protoTupleType = input.GetTuple();
            ConvertMiniKQLTupleTypeToYdbType(protoTupleType, *output.mutable_tuple_type());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Struct: {
            const NKikimrMiniKQL::TStructType& protoStructType = input.GetStruct();
            ConvertMiniKQLStructTypeToYdbType(protoStructType, *output.mutable_struct_type());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Dict: {
            const NKikimrMiniKQL::TDictType& protoDictType = input.GetDict();
            ConvertMiniKQLTypeToYdbType(protoDictType.GetKey(), *output.mutable_dict_type()->mutable_key());
            ConvertMiniKQLTypeToYdbType(protoDictType.GetPayload(), *output.mutable_dict_type()->mutable_payload());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Variant: {
            const NKikimrMiniKQL::TVariantType& protoVariantType = input.GetVariant();
            auto variantOut = output.mutable_variant_type();
            switch (protoVariantType.type_case()) {
                case NKikimrMiniKQL::TVariantType::kTupleItems: {
                    const NKikimrMiniKQL::TTupleType& protoTupleType = protoVariantType.GetTupleItems();
                    ConvertMiniKQLTupleTypeToYdbType(protoTupleType, *variantOut->mutable_tuple_items());
                    break;
                }
                case NKikimrMiniKQL::TVariantType::kStructItems: {
                    const NKikimrMiniKQL::TStructType& protoStructType = protoVariantType.GetStructItems();
                    ConvertMiniKQLStructTypeToYdbType(protoStructType, *variantOut->mutable_struct_items());
                    break;
                }
                default:
                    ythrow yexception() << "Unknown variant type representation: "
                                        << protoVariantType.DebugString();
            }
            break;
        }
        default: {
            Y_FAIL("Unknown protobuf type: %s", input.DebugString().c_str());
        }
    }
}

void ConvertYdbTypeToMiniKQLType(const Ydb::Type& input, NKikimrMiniKQL::TType& output) {
    switch (input.type_case()) {
        case Ydb::Type::kVoidType:
            output.SetKind(NKikimrMiniKQL::ETypeKind::Void);
            break;
        case Ydb::Type::kNullType:
            output.SetKind(NKikimrMiniKQL::ETypeKind::Null);
            break;
        case Ydb::Type::kTypeId:
            output.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            output.MutableData()->SetScheme(input.type_id());
            break;
        case Ydb::Type::kDecimalType: {
            // TODO: Decimal parameters
            output.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            auto data = output.MutableData();
            data->SetScheme(NYql::NProto::TypeIds::Decimal);
            data->MutableDecimalParams()->SetPrecision(input.decimal_type().precision());
            data->MutableDecimalParams()->SetScale(input.decimal_type().scale());
            break;
        }
        case Ydb::Type::kOptionalType:
            output.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
            ConvertYdbTypeToMiniKQLType(input.optional_type().item(), *output.MutableOptional()->MutableItem());
            break;
        case Ydb::Type::kListType:
            output.SetKind(NKikimrMiniKQL::ETypeKind::List);
            ConvertYdbTypeToMiniKQLType(input.list_type().item(), *output.MutableList()->MutableItem());
            break;
        case Ydb::Type::kTupleType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
            const Ydb::TupleType& protoTupleType = input.tuple_type();
            ConvertYdbTupleTypeToMiniKQLType(protoTupleType, *output.MutableTuple());
            break;
        }
        case Ydb::Type::kStructType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
            const Ydb::StructType& protoStructType = input.struct_type();
            ConvertYdbStructTypeToMiniKQLType(protoStructType, *output.MutableStruct());
            break;
        }
        case Ydb::Type::kDictType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Dict);
            const Ydb::DictType& protoDictType = input.dict_type();
            ConvertYdbTypeToMiniKQLType(protoDictType.key(), *output.MutableDict()->MutableKey());
            ConvertYdbTypeToMiniKQLType(protoDictType.payload(), *output.MutableDict()->MutablePayload());
            break;
        }
        case Ydb::Type::kVariantType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Variant);
            const Ydb::VariantType& protoVariantType = input.variant_type();
            auto variantOut = output.MutableVariant();
            switch (protoVariantType.type_case()) {
                case Ydb::VariantType::kTupleItems: {
                    const Ydb::TupleType& protoTupleType = protoVariantType.tuple_items();
                    ConvertYdbTupleTypeToMiniKQLType(protoTupleType, *variantOut->MutableTupleItems());
                    break;
                }
                case Ydb::VariantType::kStructItems: {
                    const Ydb::StructType& protoStructType = protoVariantType.struct_items();
                    ConvertYdbStructTypeToMiniKQLType(protoStructType, *variantOut->MutableStructItems());
                    break;
                }
                default:
                    ythrow yexception() << "Unknown variant type representation: "
                                        << protoVariantType.DebugString();
            }
            break;
        }
        default: {
            ythrow yexception() << "Unknown protobuf type: "
                                << input.DebugString();
        }
    }

}

Y_FORCE_INLINE void ConvertData(NUdf::TDataTypeId typeId, const NKikimrMiniKQL::TValue& value, Ydb::Value& res) {
    switch (typeId) {
        case NUdf::TDataType<bool>::Id:
            res.set_bool_value(value.GetBool());
            break;
        case NUdf::TDataType<ui8>::Id:
            res.set_uint32_value(value.GetUint32());
            break;
        case NUdf::TDataType<i8>::Id:
            res.set_int32_value(value.GetInt32());
            break;
        case NUdf::TDataType<ui16>::Id:
            res.set_uint32_value(value.GetUint32());
            break;
        case NUdf::TDataType<i16>::Id:
            res.set_int32_value(value.GetInt32());
            break;
        case NUdf::TDataType<i32>::Id:
            res.set_int32_value(value.GetInt32());
            break;
        case NUdf::TDataType<ui32>::Id:
            res.set_uint32_value(value.GetUint32());
            break;
        case NUdf::TDataType<i64>::Id:
            res.set_int64_value(value.GetInt64());
            break;
        case NUdf::TDataType<ui64>::Id:
            res.set_uint64_value(value.GetUint64());
            break;
        case NUdf::TDataType<float>::Id:
            res.set_float_value(value.GetFloat());
            break;
        case NUdf::TDataType<double>::Id:
            res.set_double_value(value.GetDouble());
            break;
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id: {
            const auto& stringRef = value.GetText();
            res.set_text_value(stringRef.data(), stringRef.size());
            break;
            }
        case NUdf::TDataType<NUdf::TDate>::Id:
            res.set_uint32_value(value.GetUint32());
            break;
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            res.set_uint32_value(value.GetUint32());
            break;
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            res.set_uint64_value(value.GetUint64());
            break;
        case NUdf::TDataType<NUdf::TInterval>::Id:
            res.set_int64_value(value.GetInt64());
            break;
        case NUdf::TDataType<NUdf::TDecimal>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            res.set_low_128(value.GetLow128());
            res.set_high_128(value.GetHi128());
            break;
        }
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            const auto json = NBinaryJson::SerializeToJson(value.GetBytes());
            res.set_text_value(json);
            break;
        }
        case NUdf::TDataType<NUdf::TDyNumber>::Id: {
            const auto number = NDyNumber::DyNumberToString(value.GetBytes());
            Y_ENSURE(number.Defined(), "Invalid DyNumber binary representation");
            res.set_text_value(*number);
            break;
        }
        default:
            const auto& stringRef = value.GetBytes();
            res.set_bytes_value(stringRef.data(), stringRef.size());
    }
}

Y_FORCE_INLINE void CheckTypeId(i32 id, i32 expected, std::string_view typeName) {
    if (id != expected) {
        throw yexception() << "Invalid value representation for type: " << typeName;
    }
}

Y_FORCE_INLINE void ConvertData(NUdf::TDataTypeId typeId, const Ydb::Value& value, NKikimrMiniKQL::TValue& res) {
    switch (typeId) {
        case NUdf::TDataType<bool>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kBoolValue, "Bool");
            res.SetBool(value.bool_value());
            break;
        case NUdf::TDataType<ui8>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint8");
            res.SetUint32(value.uint32_value());
            break;
        case NUdf::TDataType<i8>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int8");
            res.SetInt32(value.int32_value());
            break;
        case NUdf::TDataType<ui16>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint16");
            res.SetUint32(value.uint32_value());
            break;
        case NUdf::TDataType<i16>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int16");
            res.SetInt32(value.int32_value());
            break;
        case NUdf::TDataType<i32>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int32");
            res.SetInt32(value.int32_value());
            break;
        case NUdf::TDataType<ui32>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint32");
            res.SetUint32(value.uint32_value());
            break;
        case NUdf::TDataType<i64>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Int64");
            res.SetInt64(value.int64_value());
            break;
        case NUdf::TDataType<ui64>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint64Value, "Uint64");
            res.SetUint64(value.uint64_value());
            break;
        case NUdf::TDataType<float>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kFloatValue, "Float");
            res.SetFloat(value.float_value());
            break;
        case NUdf::TDataType<double>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kDoubleValue, "Double");
            res.SetDouble(value.double_value());
            break;
        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDate");
            const auto& stringRef = value.text_value();
            res.SetText(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDatetime");
            const auto& stringRef = value.text_value();
            res.SetText(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzTimestamp");
            const auto& stringRef = value.text_value();
            res.SetText(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TJson>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "Json");
            const auto& stringRef = value.text_value();
            if (!NYql::NDom::IsValidJson(stringRef)) {
                throw yexception() << "Invalid Json value";
            }
            res.SetText(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TUtf8>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "Utf8");
            const auto& stringRef = value.text_value();
            if (!NYql::IsUtf8(stringRef)) {
                throw yexception() << "Invalid Utf8 value";
            }
            res.SetText(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TDate>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Date");
            if (value.uint32_value() >= NUdf::MAX_DATE) {
                throw yexception() << "Invalid Date value";
            }
            res.SetUint32(value.uint32_value());
            break;
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Datetime");
            if (value.uint32_value() >= NUdf::MAX_DATETIME) {
                throw yexception() << "Invalid Datetime value";
            }
            res.SetUint32(value.uint32_value());
            break;
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kUint64Value, "Timestamp");
            if (value.uint64_value() >= NUdf::MAX_TIMESTAMP) {
                throw yexception() << "Invalid Timestamp value";
            }
            res.SetUint64(value.uint64_value());
            break;
        case NUdf::TDataType<NUdf::TInterval>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Interval");
            if ((ui64)std::abs(value.int64_value()) >= NUdf::MAX_TIMESTAMP) {
                throw yexception() << "Invalid Interval value";
            }
            res.SetInt64(value.int64_value());
            break;
        case NUdf::TDataType<NUdf::TUuid>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kLow128, "Uuid");
            res.SetLow128(value.low_128());
            res.SetHi128(value.high_128());
            break;
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "JsonDocument");
            const auto binaryJson = NBinaryJson::SerializeToBinaryJson(value.text_value());
            if (!binaryJson.Defined()) {
                throw yexception() << "Invalid JsonDocument value";
            }
            res.SetBytes(binaryJson->Data(), binaryJson->Size());
            break;
        }
        case NUdf::TDataType<NUdf::TDyNumber>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "DyNumber");
            const auto dyNumber = NDyNumber::ParseDyNumberString(value.text_value());
            if (!dyNumber.Defined()) {
                throw yexception() << "Invalid DyNumber value";
            }
            res.SetBytes(dyNumber->Data(), dyNumber->Size());
            break;
        }
        case NUdf::TDataType<char*>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kBytesValue, "String");
            const auto& stringRef = value.bytes_value();
            res.SetBytes(stringRef.data(), stringRef.size());
            break;
        }
        case NUdf::TDataType<NUdf::TYson>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kBytesValue, "Yson");
            const auto& stringRef = value.bytes_value();
            if (!NYql::NDom::IsValidYson(stringRef)) {
                throw yexception() << "Invalid Yson value";
            }
            res.SetBytes(stringRef.data(), stringRef.size());
            break;
        }
        default:
            throw yexception() << "Unsupported data type: " << typeId;
    }
}

void ConvertMiniKQLValueToYdbValue(const NKikimrMiniKQL::TType& inputType,
                                   const NKikimrMiniKQL::TValue& inputValue,
                                   Ydb::Value& output) {
    switch (inputType.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void: {
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Null: {
            output.set_null_flag_value(::google::protobuf::NULL_VALUE);
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Data: {
            const NKikimrMiniKQL::TDataType& protoData = inputType.GetData();
            const NUdf::TDataTypeId typeId = protoData.GetScheme();
            ConvertData(typeId, inputValue, output);
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Optional: {
            const NKikimrMiniKQL::TOptionalType& protoOptionalType = inputType.GetOptional();
            if (inputValue.HasOptional()) {
                // Optional type, and there is somthing inside - keep going
                const NKikimrMiniKQL::TType* nextType = &protoOptionalType.GetItem();
                const NKikimrMiniKQL::TValue* curValue = &inputValue;
                ui32 optionalCounter = 0;
                while (nextType->GetKind() == NKikimrMiniKQL::ETypeKind::Optional && curValue->HasOptional()) {
                    optionalCounter++;
                    nextType = &nextType->GetOptional().GetItem();
                    curValue = &curValue->GetOptional();
                }
                if (curValue->HasOptional()) {
                    // Next type is not optional and we have optional value - write this value (without nested Item messages)
                    ConvertMiniKQLValueToYdbValue(*nextType, curValue->GetOptional(), output);
                } else {
                    // Next type is not optional and we have no optional value - Optional<Optional<T>>(Null) case.
                    // Write corresponding number of nested Value messages, and NullFlag to the last one
                    Ydb::Value* resValue = &output;
                    while (optionalCounter--) {
                        resValue = resValue->mutable_nested_value();
                    }
                    resValue->set_null_flag_value(::google::protobuf::NULL_VALUE);
                }
            } else {
                // Optional type, but there isn't optional value - single empty level - convert to NullFlag
                output.set_null_flag_value(::google::protobuf::NULL_VALUE);
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::List: {
            const NKikimrMiniKQL::TListType& protoListType = inputType.GetList();
            const NKikimrMiniKQL::TType& protoItemType = protoListType.GetItem();
            for (const auto& x : inputValue.GetList()) {
                ConvertMiniKQLValueToYdbValue(protoItemType, x, *output.mutable_items()->Add());
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Struct: {
            const NKikimrMiniKQL::TStructType& protoStructType = inputType.GetStruct();
            ui32 membersCount = static_cast<ui32>(protoStructType.MemberSize());
            if (membersCount != inputValue.StructSize()) {
                ythrow yexception() << "Number of struct fields and their types mismatched";
            }
            for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
                const NKikimrMiniKQL::TMember& protoMember = protoStructType.GetMember(memberNum);
                ConvertMiniKQLValueToYdbValue(protoMember.GetType(), inputValue.GetStruct(memberNum), *output.mutable_items()->Add());
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Dict: {
            const NKikimrMiniKQL::TDictType& protoDictType = inputType.GetDict();
            for (const auto& x : inputValue.GetDict()) {
                auto pair = output.mutable_pairs()->Add();
                ConvertMiniKQLValueToYdbValue(protoDictType.GetKey(), x.GetKey(), *pair->mutable_key());
                ConvertMiniKQLValueToYdbValue(protoDictType.GetPayload(), x.GetPayload(), *pair->mutable_payload());
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Tuple: {
            const NKikimrMiniKQL::TTupleType& protoTupleType = inputType.GetTuple();
            ui32 elementsCount = static_cast<ui32>(protoTupleType.ElementSize());
            if (elementsCount != inputValue.TupleSize()) {
                throw yexception() << "Number of tuple elements and their types mismatched";
            }
            for (ui32 elementNum = 0; elementNum < elementsCount; ++elementNum) {
                const NKikimrMiniKQL::TType& protoMember = protoTupleType.GetElement(elementNum);
                ConvertMiniKQLValueToYdbValue(protoMember, inputValue.GetTuple(elementNum), *output.mutable_items()->Add());
            }
            break;
        }
        case NKikimrMiniKQL::ETypeKind::Variant: {
            const NKikimrMiniKQL::TVariantType& protoVariantType = inputType.GetVariant();
            const ui32 variantIndex = inputValue.GetVariantIndex();
            output.set_variant_index(variantIndex);
            switch (protoVariantType.type_case()) {
                case NKikimrMiniKQL::TVariantType::kTupleItems: {
                    if (variantIndex >= protoVariantType.GetTupleItems().ElementSize()) {
                        throw yexception() << "Variant index out of type range";
                    }
                    const NKikimrMiniKQL::TType& nextType = protoVariantType.GetTupleItems().GetElement(variantIndex);
                    ConvertMiniKQLValueToYdbValue(nextType, inputValue.GetOptional(), *output.mutable_nested_value());
                    break;
                }
                case NKikimrMiniKQL::TVariantType::kStructItems: {
                    if (variantIndex >= protoVariantType.GetStructItems().MemberSize()) {
                        throw yexception() << "Variant index out of type range";
                    }
                    const NKikimrMiniKQL::TType& nextType = protoVariantType.GetStructItems().GetMember(variantIndex).GetType();
                    ConvertMiniKQLValueToYdbValue(nextType, inputValue.GetOptional(), *output.mutable_nested_value());
                    break;
                }
                default:
                    ythrow yexception() << "Unknown variant type representation: "
                                        << protoVariantType.DebugString();
            }
            break;
        }
        default: {
            ythrow yexception() << "Unknown protobuf type: "
                                << inputType.DebugString();
        }
    }
}

void ConvertYdbValueToMiniKQLValue(const Ydb::Type& inputType,
                                   const Ydb::Value& inputValue,
                                   NKikimrMiniKQL::TValue& output) {

    switch (inputType.type_case()) {
        case Ydb::Type::kVoidType:
            break;
        case Ydb::Type::kTypeId:
            ConvertData(inputType.type_id(), inputValue, output);
            break;
        case Ydb::Type::kDecimalType:
            Y_ENSURE(inputValue.value_case() == Ydb::Value::kLow128);
            output.SetLow128(inputValue.low_128());
            output.SetHi128(inputValue.high_128());
            if (NYql::NDecimal::IsError(NYql::NDecimal::FromProto(output))) {
                throw yexception() << "Invalid decimal value";
            }
            break;
        case Ydb::Type::kOptionalType: {
            switch (inputValue.value_case()) {
                case Ydb::Value::kNullFlagValue:
                    break;
                case Ydb::Value::kNestedValue:
                    ConvertYdbValueToMiniKQLValue(inputType.optional_type().item(), inputValue.nested_value(), *output.MutableOptional());
                    break;
                default:
                    ConvertYdbValueToMiniKQLValue(inputType.optional_type().item(), inputValue, *output.MutableOptional());
            }
            break;
        }
        case Ydb::Type::kListType: {
            const Ydb::ListType& protoListType = inputType.list_type();
            const Ydb::Type& protoItemType = protoListType.item();
            for (const auto& x : inputValue.items()) {
                ConvertYdbValueToMiniKQLValue(protoItemType, x, *output.MutableList()->Add());
            }
            break;
        }
        case Ydb::Type::kStructType: {
            const Ydb::StructType& protoStructType = inputType.struct_type();
            ui32 membersCount = static_cast<ui32>(protoStructType.membersSize());
            if (membersCount != inputValue.itemsSize()) {
                throw yexception() << "Number of struct fields and their types mismatched";
            }
            for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
                const Ydb::StructMember& protoMember = protoStructType.members(memberNum);
                ConvertYdbValueToMiniKQLValue(protoMember.type(), inputValue.items(memberNum), *output.MutableStruct()->Add());
            }
            break;
        }
        case Ydb::Type::kDictType: {
            const Ydb::DictType& protoDictType = inputType.dict_type();
            for (const auto& x : inputValue.pairs()) {
                auto pair = output.MutableDict()->Add();
                ConvertYdbValueToMiniKQLValue(protoDictType.key(), x.key(), *pair->MutableKey());
                ConvertYdbValueToMiniKQLValue(protoDictType.payload(), x.payload(), *pair->MutablePayload());
            }
            break;
        }
        case Ydb::Type::kTupleType: {
            const Ydb::TupleType& protoTupleType = inputType.tuple_type();
            ui32 elementsCount = static_cast<ui32>(protoTupleType.elementsSize());
            if (elementsCount != inputValue.itemsSize()) {
                throw yexception() << "Number of tuple elements and their types mismatched";
            }
            for (ui32 elementNum = 0; elementNum < elementsCount; ++elementNum) {
                const Ydb::Type& elementType = protoTupleType.elements(elementNum);
                ConvertYdbValueToMiniKQLValue(elementType, inputValue.items(elementNum), *output.MutableTuple()->Add());
            }
            break;
        }
        case Ydb::Type::kVariantType: {
            const Ydb::VariantType& protoVariantType = inputType.variant_type();
            const ui32 variantIndex = inputValue.variant_index();
            output.SetVariantIndex(variantIndex);
            switch (protoVariantType.type_case()) {
                case Ydb::VariantType::kTupleItems: {
                    Y_ENSURE(protoVariantType.tuple_items().elements_size() >= 0);
                    if (variantIndex >= (ui32)protoVariantType.tuple_items().elements_size()) {
                        throw yexception() << "Variant index out of type range";
                    }
                    const Ydb::Type& nextType = protoVariantType.tuple_items().elements(variantIndex);
                    ConvertYdbValueToMiniKQLValue(nextType, inputValue.nested_value(), *output.MutableOptional());
                    break;
                }
                case Ydb::VariantType::kStructItems: {
                    Y_ENSURE(protoVariantType.struct_items().members_size() >= 0);
                    if (variantIndex >= (ui32)protoVariantType.struct_items().members_size()) {
                        throw yexception() << "Variant index out of type range";
                    }
                    const Ydb::Type& nextType = protoVariantType.struct_items().members(variantIndex).type();
                    ConvertYdbValueToMiniKQLValue(nextType, inputValue.nested_value(), *output.MutableOptional());
                    break;
                }
                default:
                    throw yexception() << "Unknown variant type representation: "
                                        << protoVariantType.DebugString();
            }
            break;
        }

        default: {
            throw yexception() << "Unknown protobuf type: "
                                << inputType.DebugString();
        }
    }
}

void ConvertYdbParamsToMiniKQLParams(const ::google::protobuf::Map<TString, Ydb::TypedValue>& input,
                                     NKikimrMiniKQL::TParams& output) {
    output.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto type = output.MutableType()->MutableStruct();
    auto value = output.MutableValue();
    for (const auto& p : input) {
        auto typeMember = type->AddMember();
        auto valueItem = value->AddStruct();
        typeMember->SetName(p.first);
        ConvertYdbTypeToMiniKQLType(p.second.type(), *typeMember->MutableType());
        ConvertYdbValueToMiniKQLValue(p.second.type(), p.second.value(), *valueItem);
    }
}

void ConvertAclToYdb(const TString& owner, const TString& acl, bool isContainer,
    google::protobuf::RepeatedPtrField<Ydb::Scheme::Permissions>* permissions) {
    const auto& securityObject = TSecurityObject(owner, acl, isContainer);
    for (const auto& ace : securityObject.GetACL().GetACE()) {
        auto entry = permissions->Add();
        entry->set_subject(ace.GetSID());
        auto str = ConvertACLMaskToYdbPermissionNames(ace.GetAccessRight());
        for (auto n : str) {
            entry->add_permission_names(n);
        }
    }

}

using namespace NACLib;

const THashMap<TString, TACLAttrs> AccessMap_  = {
    { "ydb.database.connect", TACLAttrs(EAccessRights::ConnectDatabase, EInheritanceType::InheritNone) },
    { "ydb.tables.modify", TACLAttrs(EAccessRights(UpdateRow | EraseRow)) },
    { "ydb.tables.read", TACLAttrs(EAccessRights::SelectRow | EAccessRights::ReadAttributes) },
    { "ydb.generic.read", EAccessRights::GenericRead },
    { "ydb.generic.write", EAccessRights::GenericWrite },
    { "ydb.generic.use", EAccessRights::GenericUse },
    { "ydb.generic.manage", EAccessRights::GenericManage },
    { "ydb.generic.full", EAccessRights::GenericFull },
    { "ydb.database.create", EAccessRights::CreateDatabase },
    { "ydb.database.drop", EAccessRights::DropDatabase },
    { "ydb.access.grant", EAccessRights::GrantAccessRights },
    { "ydb.deprecated.select_row", EAccessRights::SelectRow },
    { "ydb.deprecated.update_row", EAccessRights::UpdateRow },
    { "ydb.deprecated.erase_row", EAccessRights::EraseRow },
    { "ydb.deprecated.read_attributes", EAccessRights::ReadAttributes },
    { "ydb.deprecated.write_attributes", EAccessRights::WriteAttributes },
    { "ydb.deprecated.create_directory", EAccessRights::CreateDirectory },
    { "ydb.deprecated.create_table", EAccessRights::CreateTable },
    { "ydb.deprecated.create_queue", EAccessRights::CreateQueue },
    { "ydb.deprecated.remove_schema", EAccessRights::RemoveSchema },
    { "ydb.deprecated.describe_schema", EAccessRights::DescribeSchema },
    { "ydb.deprecated.alter_schema", EAccessRights::AlterSchema }

};

static ui32 BitCount(ui32 in) {
    ui32 res = 0;
    while (in) {
        res++;
        in &= in - 1;
    }
    return res;
}

static TVector<std::pair<ui32, TString>> CalcMaskByPower() {
    TVector<std::pair<ui32, TString>> result;
    for (const auto& it : AccessMap_) {
        result.push_back({it.second.AccessMask, it.first});
    }

    //Sort this vector by number of set bits in mask
    //max is first
    auto comp = [](const std::pair<ui32, TString>& a, const std::pair<ui32, TString>& b) -> bool {
        return BitCount(a.first) > BitCount(b.first);
    };
    std::sort (result.begin(), result.end(), comp);
    return result;
}

TACLAttrs ConvertYdbPermissionNameToACLAttrs(const TString& name) {
    auto it = AccessMap_.find(name);
    if (it == AccessMap_.end()) {
        throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
            << "Unknown permission name: " << name;
    }
    return it->second;
}

TVector<TString> ConvertACLMaskToYdbPermissionNames(ui32 mask) {
    static const TVector<std::pair<ui32, TString>> maskByPower = CalcMaskByPower();
    TVector<TString> result;
    for (const auto& pair : maskByPower) {
        if ((mask & pair.first ^ pair.first) == 0) {
            result.push_back(pair.second);
            mask &= ~pair.first;
        }

        if (!mask) {
            break;
        }
    }
    return result;
}

void ConvertDirectoryEntry(const NKikimrSchemeOp::TDirEntry& from, Ydb::Scheme::Entry* to, bool processAcl) {
    to->set_name(from.GetName());
    to->set_owner(from.GetOwner());

    switch (from.GetPathType()) {
    case NKikimrSchemeOp::EPathTypeExtSubDomain:
        to->set_type(static_cast<Ydb::Scheme::Entry::Type>(NKikimrSchemeOp::EPathTypeSubDomain));
        break;
    case NKikimrSchemeOp::EPathTypePersQueueGroup:
        to->set_type(Ydb::Scheme::Entry::TOPIC);
        break;

    default:
        to->set_type(static_cast<Ydb::Scheme::Entry::Type>(from.GetPathType()));
    }

    if (processAcl) {
        const bool isDir = from.GetPathType() == NKikimrSchemeOp::EPathTypeDir;
        ConvertAclToYdb(from.GetOwner(), from.GetEffectiveACL(), isDir, to->mutable_effective_permissions());
        ConvertAclToYdb(from.GetOwner(), from.GetACL(), isDir, to->mutable_permissions());
    }
}

void ConvertDirectoryEntry(const NKikimrSchemeOp::TPathDescription& from, Ydb::Scheme::Entry* to, bool processAcl) {
    ConvertDirectoryEntry(from.GetSelf(), to, processAcl);

    switch (from.GetSelf().GetPathType()) {
    case NKikimrSchemeOp::EPathTypeTable:
        to->set_size_bytes(from.GetTableStats().GetDataSize() + from.GetTableStats().GetIndexSize());
        for (const auto& index : from.GetTable().GetTableIndexes()) {
            to->set_size_bytes(to->size_bytes() + index.GetDataSize());
        }
        break;
    case NKikimrSchemeOp::EPathTypeSubDomain:
    case NKikimrSchemeOp::EPathTypeExtSubDomain:
        to->set_size_bytes(from.GetDomainDescription().GetDiskSpaceUsage().GetTables().GetTotalSize());
        break;
    case NKikimrSchemeOp::EPathTypePersQueueGroup:
        to->set_type(Ydb::Scheme::Entry::TOPIC);
        break;

    default:
        break;
    }
}

void ConvertYdbResultToKqpResult(const Ydb::ResultSet& input, NKikimrMiniKQL::TResult& output) {
    auto& outputType = *output.MutableType();
    outputType.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto& dataMember = *outputType.MutableStruct()->AddMember();
    dataMember.SetName("Data");
    auto& truncatedMember = *outputType.MutableStruct()->AddMember();
    truncatedMember.SetName("Truncated");
    truncatedMember.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
    truncatedMember.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<bool>::Id);

    auto& dataType = *dataMember.MutableType();
    dataType.SetKind(NKikimrMiniKQL::ETypeKind::List);
    auto& itemType = *dataType.MutableList()->MutableItem();
    itemType.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto& structType = *itemType.MutableStruct();

    auto& outputValue = *output.MutableValue();
    auto& dataValue = *outputValue.AddStruct();
    auto& truncatedValue = *outputValue.AddStruct();

    for (auto& column : input.columns()) {
        auto& columnMember = *structType.AddMember();
        columnMember.SetName(column.name());
        ConvertYdbTypeToMiniKQLType(column.type(), *columnMember.MutableType());
    }

    Ydb::Type ydbRowType;
    ConvertMiniKQLTypeToYdbType(itemType, ydbRowType);

    for (auto& row : input.rows()) {
        ConvertYdbValueToMiniKQLValue(ydbRowType, row, *dataValue.AddList());
    }

    truncatedValue.SetBool(input.truncated());
}

TACLAttrs::TACLAttrs(ui32 access, ui32 inheritance)
    : AccessMask(access)
    , InheritanceType(inheritance)
{}

TACLAttrs::TACLAttrs(ui32 access)
    : AccessMask(access)
    , InheritanceType(EInheritanceType::InheritObject | EInheritanceType::InheritContainer)
{}

} // namespace NKikimr
