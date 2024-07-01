#include "ydb_convert.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/library/binary_json/read.h>
#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>

#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
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
            Y_ABORT_UNLESS(NUdf::FindDataSlot(schemeType), "unknown type id: %d", (int) schemeType);
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
        case NKikimrMiniKQL::ETypeKind::Tagged: {
            const NKikimrMiniKQL::TTaggedType& protoTaggedType = input.GetTagged();
            output.mutable_tagged_type()->set_tag(protoTaggedType.GetTag());
            ConvertMiniKQLTypeToYdbType(protoTaggedType.GetItem(), *output.mutable_tagged_type()->mutable_type());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::List: {
            const NKikimrMiniKQL::TListType& protoListType = input.GetList();
            ConvertMiniKQLTypeToYdbType(protoListType.GetItem(), *output.mutable_list_type()->mutable_item());
            break;
        }
        case NKikimrMiniKQL::ETypeKind::EmptyList: {
            output.set_empty_list_type(::google::protobuf::NULL_VALUE);
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
        case NKikimrMiniKQL::ETypeKind::EmptyDict: {
            output.set_empty_dict_type(::google::protobuf::NULL_VALUE);
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
        case NKikimrMiniKQL::ETypeKind::Pg: {
            const NKikimrMiniKQL::TPgType& pgType = input.GetPg();
            auto pgOut = output.mutable_pg_type();
            pgOut->Setoid(pgType.Getoid());
            break;
        }
        default: {
            Y_ABORT("Unknown protobuf type: %s", input.DebugString().c_str());
        }
    }
}

void ConvertYdbTypeToMiniKQLType(const Ydb::Type& input, NKikimrMiniKQL::TType& output) {
    switch (input.type_case()) {
        case Ydb::Type::kVoidType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Void);
            break;
        }
        case Ydb::Type::kNullType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Null);
            break;
        }
        case Ydb::Type::kTypeId: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            output.MutableData()->SetScheme(input.type_id());
            break;
        }
        case Ydb::Type::kDecimalType: {
            // TODO: Decimal parameters
            output.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            auto data = output.MutableData();
            data->SetScheme(NYql::NProto::TypeIds::Decimal);
            data->MutableDecimalParams()->SetPrecision(input.decimal_type().precision());
            data->MutableDecimalParams()->SetScale(input.decimal_type().scale());
            break;
        }
        case Ydb::Type::kOptionalType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
            ConvertYdbTypeToMiniKQLType(input.optional_type().item(), *output.MutableOptional()->MutableItem());
            break;
        }
        case Ydb::Type::kListType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::List);
            ConvertYdbTypeToMiniKQLType(input.list_type().item(), *output.MutableList()->MutableItem());
            break;
        }
        case Ydb::Type::kEmptyListType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::EmptyList);
            break;
        }
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
        case Ydb::Type::kEmptyDictType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::EmptyDict);
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
        case Ydb::Type::kPgType: {
            output.SetKind(NKikimrMiniKQL::ETypeKind::Pg);
            const Ydb::PgType& pgType = input.pg_type();
            auto pgOut = output.MutablePg();
            pgOut->Setoid(pgType.Getoid());
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
        case NUdf::TDataType<NUdf::TDate32>::Id:
            res.set_int32_value(value.GetInt32());
            break;
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
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
        case NUdf::TDataType<NUdf::TDate32>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Date");
            if (value.int32_value() >= NUdf::MAX_DATE32) {
                throw yexception() << "Invalid Date32 value";
            }
            res.SetInt32(value.int32_value());
            break;
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Datetime");
            if (value.int64_value() >= NUdf::MAX_DATETIME64) {
                throw yexception() << "Invalid Datetime64 value";
            }
            res.SetInt64(value.int64_value());
            break;
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Timestamp");
            if (value.int64_value() >= NUdf::MAX_TIMESTAMP64) {
                throw yexception() << "Invalid Timestamp64 value";
            }
            res.SetInt64(value.int64_value());
            break;
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Interval");
            if (std::abs(value.int64_value()) >= NUdf::MAX_INTERVAL64) {
                throw yexception() << "Invalid Interval64 value";
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
        case NKikimrMiniKQL::ETypeKind::EmptyList: {
            break;
        }
        case NKikimrMiniKQL::ETypeKind::EmptyDict: {
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
        case NKikimrMiniKQL::ETypeKind::Tagged: {
            const NKikimrMiniKQL::TTaggedType& protoTaggedType = inputType.GetTagged();
            const NKikimrMiniKQL::TType& protoItemType = protoTaggedType.GetItem();
            ConvertMiniKQLValueToYdbValue(protoItemType, inputValue, output);
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
        case NKikimrMiniKQL::ETypeKind::Pg: {
            if (inputValue.GetValueValueCase() == NKikimrMiniKQL::TValue::kNullFlagValue) {
                output.Setnull_flag_value(::google::protobuf::NULL_VALUE);
            } else if (inputValue.HasBytes()) {
                const auto& stringRef = inputValue.GetBytes();
                output.set_bytes_value(stringRef.data(), stringRef.size());
            } else if (inputValue.HasText()) {
                const auto& stringRef = inputValue.GetText();
                output.set_text_value(stringRef.data(), stringRef.size());
            } else {
                Y_ENSURE(false, "malformed pg value");
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
        case Ydb::Type::kEmptyListType:
            break;
        case Ydb::Type::kEmptyDictType:
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
        case Ydb::Type::kPgType: {
            const auto& stringRef = inputValue.Gettext_value();
            output.SetText(stringRef.data(), stringRef.size());
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

namespace {

const TString YDB_DATABASE_CONNECT = "ydb.database.connect";
const TString YDB_TABLES_MODIFY = "ydb.tables.modify";
const TString YDB_TABLES_READ = "ydb.tables.read";
const TString YDB_GENERIC_LIST = "ydb.generic.list";
const TString YDB_GENERIC_READ = "ydb.generic.read";
const TString YDB_GENERIC_WRITE = "ydb.generic.write";
const TString YDB_GENERIC_USE_LEGACY = "ydb.generic.use_legacy";
const TString YDB_GENERIC_USE = "ydb.generic.use";
const TString YDB_GENERIC_MANAGE = "ydb.generic.manage";
const TString YDB_GENERIC_FULL_LEGACY = "ydb.generic.full_legacy";
const TString YDB_GENERIC_FULL = "ydb.generic.full";
const TString YDB_DATABASE_CREATE = "ydb.database.create";
const TString YDB_DATABASE_DROP = "ydb.database.drop";
const TString YDB_ACCESS_GRANT = "ydb.access.grant";
const TString YDB_GRANULAR_SELECT_ROW = "ydb.granular.select_row";
const TString YDB_GRANULAR_UPDATE_ROW = "ydb.granular.update_row";
const TString YDB_GRANULAR_ERASE_ROW = "ydb.granular.erase_row";
const TString YDB_GRANULAR_READ_ATTRIBUTES = "ydb.granular.read_attributes";
const TString YDB_GRANULAR_WRITE_ATTRIBUTES = "ydb.granular.write_attributes";
const TString YDB_GRANULAR_CREATE_DIRECTORY = "ydb.granular.create_directory";
const TString YDB_GRANULAR_CREATE_TABLE = "ydb.granular.create_table";
const TString YDB_GRANULAR_CREATE_QUEUE = "ydb.granular.create_queue";
const TString YDB_GRANULAR_REMOVE_SCHEMA = "ydb.granular.remove_schema";
const TString YDB_GRANULAR_DESCRIBE_SCHEMA = "ydb.granular.describe_schema";
const TString YDB_GRANULAR_ALTER_SCHEMA = "ydb.granular.alter_schema";

const TString& GetAclName(const TString& name) {
    static const THashMap<TString, TString> GranularNamesMap_ = {
        { "ydb.deprecated.select_row", YDB_GRANULAR_SELECT_ROW },
        { "ydb.deprecated.update_row", YDB_GRANULAR_UPDATE_ROW },
        { "ydb.deprecated.erase_row", YDB_GRANULAR_ERASE_ROW },
        { "ydb.deprecated.read_attributes", YDB_GRANULAR_READ_ATTRIBUTES },
        { "ydb.deprecated.write_attributes", YDB_GRANULAR_WRITE_ATTRIBUTES },
        { "ydb.deprecated.create_directory", YDB_GRANULAR_CREATE_DIRECTORY },
        { "ydb.deprecated.create_table", YDB_GRANULAR_CREATE_TABLE },
        { "ydb.deprecated.create_queue", YDB_GRANULAR_CREATE_QUEUE },
        { "ydb.deprecated.remove_schema", YDB_GRANULAR_REMOVE_SCHEMA },
        { "ydb.deprecated.describe_schema", YDB_GRANULAR_DESCRIBE_SCHEMA },
        { "ydb.deprecated.alter_schema", YDB_GRANULAR_ALTER_SCHEMA }
    };
    auto it = GranularNamesMap_.find(name);
    return it != GranularNamesMap_.cend() ? it->second : name;
}

} // namespace

const THashMap<TString, TACLAttrs> AccessMap_  = {
    { YDB_DATABASE_CONNECT, TACLAttrs(EAccessRights::ConnectDatabase, EInheritanceType::InheritNone) },
    { YDB_TABLES_MODIFY, TACLAttrs(EAccessRights(UpdateRow | EraseRow)) },
    { YDB_TABLES_READ, TACLAttrs(EAccessRights::SelectRow | EAccessRights::ReadAttributes) },
    { YDB_GENERIC_LIST, EAccessRights::GenericList},
    { YDB_GENERIC_READ, EAccessRights::GenericRead },
    { YDB_GENERIC_WRITE, EAccessRights::GenericWrite },
    { YDB_GENERIC_USE_LEGACY, EAccessRights::GenericUseLegacy },
    { YDB_GENERIC_USE, EAccessRights::GenericUse},
    { YDB_GENERIC_MANAGE, EAccessRights::GenericManage },
    { YDB_GENERIC_FULL_LEGACY, EAccessRights::GenericFullLegacy},
    { YDB_GENERIC_FULL, EAccessRights::GenericFull },
    { YDB_DATABASE_CREATE, EAccessRights::CreateDatabase },
    { YDB_DATABASE_DROP, EAccessRights::DropDatabase },
    { YDB_ACCESS_GRANT, EAccessRights::GrantAccessRights },
    { YDB_GRANULAR_SELECT_ROW, EAccessRights::SelectRow },
    { YDB_GRANULAR_UPDATE_ROW, EAccessRights::UpdateRow },
    { YDB_GRANULAR_ERASE_ROW, EAccessRights::EraseRow },
    { YDB_GRANULAR_READ_ATTRIBUTES, EAccessRights::ReadAttributes },
    { YDB_GRANULAR_WRITE_ATTRIBUTES, EAccessRights::WriteAttributes },
    { YDB_GRANULAR_CREATE_DIRECTORY, EAccessRights::CreateDirectory },
    { YDB_GRANULAR_CREATE_TABLE, EAccessRights::CreateTable },
    { YDB_GRANULAR_CREATE_QUEUE, EAccessRights::CreateQueue },
    { YDB_GRANULAR_REMOVE_SCHEMA, EAccessRights::RemoveSchema },
    { YDB_GRANULAR_DESCRIBE_SCHEMA, EAccessRights::DescribeSchema },
    { YDB_GRANULAR_ALTER_SCHEMA, EAccessRights::AlterSchema }

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
    auto it = AccessMap_.find(GetAclName(name));
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

TString ConvertShortYdbPermissionNameToFullYdbPermissionName(const TString& name) {
    static const THashMap<TString, TString> shortPermissionNames {
        {"connect", YDB_DATABASE_CONNECT},
        {"modify_tables", YDB_TABLES_MODIFY},
        {"select_tables", YDB_TABLES_READ},
        {"list", YDB_GENERIC_LIST},
        {"select", YDB_GENERIC_READ},
        {"insert", YDB_GENERIC_WRITE},
        {"use_legacy", YDB_GENERIC_USE_LEGACY},
        {"use", YDB_GENERIC_USE},
        {"manage", YDB_GENERIC_MANAGE},
        {"full_legacy", YDB_GENERIC_FULL_LEGACY},
        {"full", YDB_GENERIC_FULL},
        {"create", YDB_DATABASE_CREATE},
        {"drop", YDB_DATABASE_DROP},
        {"grant", YDB_ACCESS_GRANT},
        {"select_row", YDB_GRANULAR_SELECT_ROW},
        {"update_row", YDB_GRANULAR_UPDATE_ROW},
        {"erase_row", YDB_GRANULAR_ERASE_ROW},
        {"select_attributes", YDB_GRANULAR_READ_ATTRIBUTES},
        {"modify_attributes", YDB_GRANULAR_WRITE_ATTRIBUTES},
        {"create_directory", YDB_GRANULAR_CREATE_DIRECTORY},
        {"create_table", YDB_GRANULAR_CREATE_TABLE},
        {"create_queue", YDB_GRANULAR_CREATE_QUEUE},
        {"remove_schema", YDB_GRANULAR_REMOVE_SCHEMA},
        {"describe_schema", YDB_GRANULAR_DESCRIBE_SCHEMA},
        {"alter_schema", YDB_GRANULAR_ALTER_SCHEMA}
    };

    const auto it = shortPermissionNames.find(to_lower(name));
    return it != shortPermissionNames.cend() ? it->second : name;
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

    auto& timestamp = *to->mutable_created_at();
    timestamp.set_plan_step(from.GetCreateStep());
    timestamp.set_tx_id(from.GetCreateTxId());

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

bool CheckValueData(NScheme::TTypeInfo type, const TCell& cell, TString& err) {
    bool ok = true;
    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Bool:
    case NScheme::NTypeIds::Int8:
    case NScheme::NTypeIds::Uint8:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Int64:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::Float:
    case NScheme::NTypeIds::Double:
    case NScheme::NTypeIds::String:
        break;

    case NScheme::NTypeIds::Decimal:
        ok = !NYql::NDecimal::IsError(cell.AsValue<NYql::NDecimal::TInt128>());
        break;

    case NScheme::NTypeIds::Date:
        ok = cell.AsValue<ui16>() < NUdf::MAX_DATE;
        break;

    case NScheme::NTypeIds::Datetime:
        ok = cell.AsValue<ui32>() < NUdf::MAX_DATETIME;
        break;

    case NScheme::NTypeIds::Timestamp:
        ok = cell.AsValue<ui64>() < NUdf::MAX_TIMESTAMP;
        break;

    case NScheme::NTypeIds::Interval:
        ok = (ui64)std::abs(cell.AsValue<i64>()) < NUdf::MAX_TIMESTAMP;
        break;

    case NScheme::NTypeIds::Date32:
        ok = cell.AsValue<i32>() < NUdf::MAX_DATE32;
        break;

    case NScheme::NTypeIds::Datetime64:
        ok = cell.AsValue<i64>() < NUdf::MAX_DATETIME64;
        break;

    case NScheme::NTypeIds::Timestamp64:
        ok = cell.AsValue<i64>() < NUdf::MAX_TIMESTAMP64;
        break;

    case NScheme::NTypeIds::Interval64:
        ok = std::abs(cell.AsValue<i64>()) < NUdf::MAX_INTERVAL64;
        break;        

    case NScheme::NTypeIds::Utf8:
        ok = NYql::IsUtf8(cell.AsBuf());
        break;

    case NScheme::NTypeIds::Yson:
        ok = NYql::NDom::IsValidYson(cell.AsBuf());
        break;

    case NScheme::NTypeIds::Json:
        ok = NYql::NDom::IsValidJson(cell.AsBuf());
        break;

    case NScheme::NTypeIds::JsonDocument:
        // JsonDocument value was verified at parsing time
        break;

    case NScheme::NTypeIds::DyNumber:
        // DyNumber value was verified at parsing time
        break;

    case NScheme::NTypeIds::Uuid:
        // Uuid value was verified at parsing time
        break;

    case NScheme::NTypeIds::Pg:
        // no pg validation here
        break;

    default:
        err = Sprintf("Unexpected type %d", type.GetTypeId());
        return false;
    }

    if (!ok) {
        err = Sprintf("Invalid %s value", NScheme::TypeName(type).c_str());
    }

    return ok;
}

bool CellFromProtoVal(NScheme::TTypeInfo type, i32 typmod, const Ydb::Value* vp,
                                TCell& c, TString& err, TMemoryPool& valueDataPool)
{
    if (vp->Hasnull_flag_value()) {
        c = TCell();
        return true;
    }

    if (vp->Hasnested_value()) {
        vp = &vp->Getnested_value();
    }

    const Ydb::Value& val = *vp;

#define EXTRACT_VAL(cellType, protoType, cppType) \
    case NScheme::NTypeIds::cellType : { \
            cppType v = val.Get##protoType##_value(); \
            c = TCell((const char*)&v, sizeof(v)); \
            break; \
        }

    switch (type.GetTypeId()) {
    EXTRACT_VAL(Bool, bool, ui8);
    EXTRACT_VAL(Int8, int32, i8);
    EXTRACT_VAL(Uint8, uint32, ui8);
    EXTRACT_VAL(Int16, int32, i16);
    EXTRACT_VAL(Uint16, uint32, ui16);
    EXTRACT_VAL(Int32, int32, i32);
    EXTRACT_VAL(Uint32, uint32, ui32);
    EXTRACT_VAL(Int64, int64, i64);
    EXTRACT_VAL(Uint64, uint64, ui64);
    EXTRACT_VAL(Float, float, float);
    EXTRACT_VAL(Double, double, double);
    EXTRACT_VAL(Date, uint32, ui16);
    EXTRACT_VAL(Datetime, uint32, ui32);
    EXTRACT_VAL(Timestamp, uint64, ui64);
    EXTRACT_VAL(Interval, int64, i64);
    EXTRACT_VAL(Date32, int32, i32);
    EXTRACT_VAL(Datetime64, int64, i64);
    EXTRACT_VAL(Timestamp64, int64, i64);
    EXTRACT_VAL(Interval64, int64, i64);
    case NScheme::NTypeIds::Json :
    case NScheme::NTypeIds::Utf8 : {
            TString v = val.Gettext_value();
            c = TCell(v.data(), v.size());
            break;
        }
    case NScheme::NTypeIds::JsonDocument : {
        const auto binaryJson = NBinaryJson::SerializeToBinaryJson(val.Gettext_value());
        if (!binaryJson.Defined()) {
            err = "Invalid JSON for JsonDocument provided";
            return false;
        }
        const auto binaryJsonInPool = valueDataPool.AppendString(TStringBuf(binaryJson->Data(), binaryJson->Size()));
        c = TCell(binaryJsonInPool.data(), binaryJsonInPool.size());
        break;
    }
    case NScheme::NTypeIds::DyNumber : {
        const auto dyNumber = NDyNumber::ParseDyNumberString(val.Gettext_value());
        if (!dyNumber.Defined()) {
            err = "Invalid DyNumber string representation";
            return false;
        }
        const auto dyNumberInPool = valueDataPool.AppendString(TStringBuf(*dyNumber));
        c = TCell(dyNumberInPool.data(), dyNumberInPool.size());
        break;
    }
    case NScheme::NTypeIds::Yson :
    case NScheme::NTypeIds::String : {
            TString v = val.Getbytes_value();
            c = TCell(v.data(), v.size());
            break;
        }
    case NScheme::NTypeIds::Decimal :
    case NScheme::NTypeIds::Uuid : {
        std::pair<ui64,ui64>& valInPool = *valueDataPool.Allocate<std::pair<ui64,ui64> >();
        valInPool.first = val.low_128();
        valInPool.second = val.high_128();
        c = TCell((const char*)&valInPool, sizeof(valInPool));
        break;
    }
    case NScheme::NTypeIds::Pg : {
        TString binary;
        bool isText = false;
        TString text = val.Gettext_value();
        if (!text.empty()) {
            isText = true;
            auto desc = type.GetTypeDesc();
            auto res = NPg::PgNativeBinaryFromNativeText(text, desc);
            if (res.Error) {
                err = TStringBuilder() << "Invalid text value for "
                    << NPg::PgTypeNameFromTypeDesc(desc) << ": " << *res.Error;
                return false;
            }
            binary = res.Str;
        } else {
            binary = val.Getbytes_value();
        }
        auto* desc = type.GetTypeDesc();
        if (typmod != -1 && NPg::TypeDescNeedsCoercion(desc)) {
            auto res = NPg::PgNativeBinaryCoerce(TStringBuf(binary), desc, typmod);
            if (res.Error) {
                err = TStringBuilder() << "Unable to coerce value for "
                    << NPg::PgTypeNameFromTypeDesc(desc) << ": " << *res.Error;
                return false;
            }
            if (res.NewValue) {
                const auto valueInPool = valueDataPool.AppendString(TStringBuf(*res.NewValue));
                c = TCell(valueInPool.data(), valueInPool.size());
            } else if (isText) {
                const auto valueInPool = valueDataPool.AppendString(TStringBuf(binary));
                c = TCell(valueInPool.data(), valueInPool.size());
            } else {
                c = TCell(binary.data(), binary.size());
            }
        } else {
            auto error = NPg::PgNativeBinaryValidate(TStringBuf(binary), desc);
            if (error) {
                err = TStringBuilder() << "Invalid binary value for "
                    << NPg::PgTypeNameFromTypeDesc(desc) << ": " << *error;
                return false;
            }
            if (isText) {
                const auto valueInPool = valueDataPool.AppendString(TStringBuf(binary));
                c = TCell(valueInPool.data(), valueInPool.size());
            } else {
                c = TCell(binary.data(), binary.size());
            }
        }
        break;
    }
    default:
        err = Sprintf("Unexpected type %d", type.GetTypeId());
        return false;
    };

    return CheckValueData(type, c, err);
}

void ProtoValueFromCell(NYdb::TValueBuilder& vb, const NScheme::TTypeInfo& typeInfo, const TCell& cell) {
    auto getString = [&cell] () {
        return TString(cell.AsBuf().data(), cell.AsBuf().size());
    };
    using namespace NYdb;
    auto primitive = (NYdb::EPrimitiveType)typeInfo.GetTypeId();
    switch (primitive) {
    case EPrimitiveType::Bool:
        vb.Bool(cell.AsValue<bool>());
        break;
    case EPrimitiveType::Int8:
        vb.Int8(cell.AsValue<i8>());
        break;
    case EPrimitiveType::Uint8:
        vb.Uint8(cell.AsValue<ui8>());
        break;
    case EPrimitiveType::Int16:
        vb.Int16(cell.AsValue<i16>());
        break;
    case EPrimitiveType::Uint16:
        vb.Uint16(cell.AsValue<ui16>());
        break;
    case EPrimitiveType::Int32:
        vb.Int32(cell.AsValue<i32>());
        break;
    case EPrimitiveType::Uint32:
        vb.Uint32(cell.AsValue<ui32>());
        break;
    case EPrimitiveType::Int64:
        vb.Int64(cell.AsValue<i64>());
        break;
    case EPrimitiveType::Uint64:
        vb.Uint64(cell.AsValue<ui64>());
        break;
    case EPrimitiveType::Float:
        vb.Float(cell.AsValue<float>());
        break;
    case EPrimitiveType::Double:
        vb.Double(cell.AsValue<double>());
        break;
    case EPrimitiveType::Date:
        vb.Date(TInstant::Days(cell.AsValue<ui16>()));
        break;
    case EPrimitiveType::Datetime:
        vb.Datetime(TInstant::Seconds(cell.AsValue<ui32>()));
        break;
    case EPrimitiveType::Timestamp:
        vb.Timestamp(TInstant::MicroSeconds(cell.AsValue<ui64>()));
        break;
    case EPrimitiveType::Interval:
        vb.Interval(cell.AsValue<i64>());
        break;
    case EPrimitiveType::Date32:
        vb.Date32(cell.AsValue<i32>());
        break;
    case EPrimitiveType::Datetime64:
        vb.Datetime64(cell.AsValue<i64>());
        break;
    case EPrimitiveType::Timestamp64:
        vb.Timestamp64(cell.AsValue<i64>());
        break;
    case EPrimitiveType::Interval64:
        vb.Interval64(cell.AsValue<i64>());
        break;        
    case EPrimitiveType::TzDate:
        vb.TzDate(getString());
        break;
    case EPrimitiveType::TzDatetime:
        vb.TzDatetime(getString());
        break;
    case EPrimitiveType::TzTimestamp:
        vb.TzTimestamp(getString());
        break;
    case EPrimitiveType::String:
        vb.String(getString());
        break;
    case EPrimitiveType::Utf8:
        vb.Utf8(getString());
        break;
    case EPrimitiveType::Yson:
        vb.Yson(getString());
        break;
    case EPrimitiveType::Json:
        vb.Json(getString());
        break;
    case EPrimitiveType::Uuid: {
        ui64 hi;
        ui64 lo;
        NUuid::UuidBytesToHalfs(cell.AsBuf().Data(), 16, hi, lo);
        vb.Uuid(TUuidValue(lo, hi));
        break;
    }
    case EPrimitiveType::JsonDocument:
        vb.JsonDocument(NBinaryJson::SerializeToJson(getString()));
        break;
    case EPrimitiveType::DyNumber:
        vb.DyNumber(getString());
        break;
    default:
        Y_ENSURE(false, TStringBuilder() << "Unsupported type: " << primitive);
    }
}

} // namespace NKikimr
