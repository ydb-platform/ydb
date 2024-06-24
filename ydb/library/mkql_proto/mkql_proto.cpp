#include "mkql_proto.h"

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr::NMiniKQL {

namespace {

void ExportTypeToProtoImpl(TType* type, NKikimrMiniKQL::TType& res, const TVector<ui32>* columnOrder = nullptr);

Y_FORCE_INLINE void HandleKindDataExport(const TType* type, const NUdf::TUnboxedValuePod& value, Ydb::Value& res) {
    auto dataType = static_cast<const TDataType*>(type);
    switch (dataType->GetSchemeType()) {
        case NUdf::TDataType<bool>::Id:
            res.set_bool_value(value.Get<bool>());
            break;
        case NUdf::TDataType<ui8>::Id:
            res.set_uint32_value(value.Get<ui8>());
            break;
        case NUdf::TDataType<i8>::Id:
            res.set_int32_value(value.Get<i8>());
            break;
        case NUdf::TDataType<ui16>::Id:
            res.set_uint32_value(value.Get<ui16>());
            break;
        case NUdf::TDataType<i16>::Id:
            res.set_int32_value(value.Get<i16>());
            break;
        case NUdf::TDataType<i32>::Id:
        case NUdf::TDataType<NUdf::TDate32>::Id:
            res.set_int32_value(value.Get<i32>());
            break;
        case NUdf::TDataType<ui32>::Id:
            res.set_uint32_value(value.Get<ui32>());
            break;
        case NUdf::TDataType<i64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            res.set_int64_value(value.Get<i64>());
            break;
        case NUdf::TDataType<ui64>::Id:
            res.set_uint64_value(value.Get<ui64>());
            break;
        case NUdf::TDataType<float>::Id:
            res.set_float_value(value.Get<float>());
            break;
        case NUdf::TDataType<double>::Id:
            res.set_double_value(value.Get<double>());
            break;
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id: {
            const auto& stringRef = value.AsStringRef();
            res.set_text_value(stringRef.Data(), stringRef.Size());
            break;
            }
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: 
        case NUdf::TDataType<NUdf::TTzDate32>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            const NUdf::TUnboxedValue out(ValueToString(NUdf::GetDataSlot(dataType->GetSchemeType()), value));
            const auto& stringRef = out.AsStringRef();
            res.set_text_value(stringRef.Data(), stringRef.Size());
            break;
            }
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            auto decimal = value.GetInt128();
            const auto p = reinterpret_cast<ui8*>(&decimal);
            res.set_low_128(*reinterpret_cast<ui64*>(p));
            res.set_high_128(*reinterpret_cast<ui64*>(p+8));
            break;
            }
        case NUdf::TDataType<NUdf::TDate>::Id:
            res.set_uint32_value(value.Get<ui16>());
            break;
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            res.set_uint32_value(value.Get<ui32>());
            break;
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            res.set_uint64_value(value.Get<ui64>());
            break;
        case NUdf::TDataType<NUdf::TInterval>::Id:
            res.set_int64_value(value.Get<i64>());
            break;
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            const auto& stringRef = value.AsStringRef();
            UuidToYdbProto(stringRef.Data(), stringRef.Size(), res);
            break;
        }
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            NUdf::TUnboxedValue json = ValueToString(NUdf::EDataSlot::JsonDocument, value);
            const auto stringRef = json.AsStringRef();
            res.set_text_value(stringRef.Data(), stringRef.Size());
            break;
        }
        case NUdf::TDataType<NUdf::TDyNumber>::Id: {
            NUdf::TUnboxedValue number = ValueToString(NUdf::EDataSlot::DyNumber, value);
            const auto stringRef = number.AsStringRef();
            res.set_text_value(stringRef.Data(), stringRef.Size());
            break;
        }
        default:
            const auto& stringRef = value.AsStringRef();
            res.set_bytes_value(stringRef.Data(), stringRef.Size());
    }
}

template<typename TOut>
Y_FORCE_INLINE void ExportStructTypeToProto(TStructType* structType, TOut& res, const TVector<ui32>* columnOrder) {
    for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
        auto newMember = res.AddMember();
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? index : (*columnOrder)[index];
        newMember->SetName(TString(structType->GetMemberName(memberIndex)));
        ExportTypeToProtoImpl(structType->GetMemberType(memberIndex), *newMember->MutableType());
    }
}

template<typename TOut>
Y_FORCE_INLINE void ExportTupleTypeToProto(TTupleType* tupleType, TOut& res) {
    for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
        ExportTypeToProtoImpl(tupleType->GetElementType(index), *res.AddElement());
    }
}

void ExportTypeToProtoImpl(TType* type, NKikimrMiniKQL::TType& res, const TVector<ui32>* columnOrder) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
            res.SetKind(NKikimrMiniKQL::ETypeKind::Void);
            break;

        case TType::EKind::Null:
            res.SetKind(NKikimrMiniKQL::ETypeKind::Null);
            break;

        case TType::EKind::Data: {
            auto dataType = static_cast<TDataType *>(type);
            auto schemeType = dataType->GetSchemeType();
            res.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            res.MutableData()->SetScheme(schemeType);
            if (schemeType == NYql::NProto::TypeIds::Decimal) {
                auto decimalType = static_cast<TDataDecimalType *>(dataType);
                auto params = decimalType->GetParams();
                auto decimalParams = res.MutableData()->MutableDecimalParams();
                decimalParams->SetPrecision(params.first);
                decimalParams->SetScale(params.second);
            }
            break;
        }

        case TType::EKind::Pg: {
            auto pgType = static_cast<TPgType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Pg);
            res.MutablePg()->set_oid(pgType->GetTypeId());
            break;
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<TTaggedType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Tagged);
            res.MutableTagged()->SetTag(TString(taggedType->GetTag()));
            ExportTypeToProtoImpl(taggedType->GetBaseType(), *res.MutableTagged()->MutableItem());
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<TOptionalType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
            ExportTypeToProtoImpl(optionalType->GetItemType(), *res.MutableOptional()->MutableItem());
            break;
        }

        case TType::EKind::EmptyList: {
            res.SetKind(NKikimrMiniKQL::ETypeKind::EmptyList);
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<TListType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::List);
            ExportTypeToProtoImpl(listType->GetItemType(), *res.MutableList()->MutableItem());
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
            if (structType->GetMembersCount()) {
                ExportStructTypeToProto(structType, *res.MutableStruct(), columnOrder);
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
            if (tupleType->GetElementsCount()) {
                ExportTupleTypeToProto(tupleType, *res.MutableTuple());
            }
            break;
        }

        case TType::EKind::EmptyDict: {
            res.SetKind(NKikimrMiniKQL::ETypeKind::EmptyDict);
            break;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<TDictType *>(type);
            res.SetKind(NKikimrMiniKQL::ETypeKind::Dict);
            ExportTypeToProtoImpl(dictType->GetKeyType(), *res.MutableDict()->MutableKey());
            ExportTypeToProtoImpl(dictType->GetPayloadType(), *res.MutableDict()->MutablePayload());
            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<TVariantType *>(type);
            TType *innerType = variantType->GetUnderlyingType();
            res.SetKind(NKikimrMiniKQL::ETypeKind::Variant);
            if (innerType->IsStruct()) {
                TStructType *structType = static_cast<TStructType *>(innerType);
                auto resItems = res.MutableVariant()->MutableStructItems();
                if (structType->GetMembersCount()) {
                    ExportStructTypeToProto(structType, *resItems, nullptr);
                }
            } else if (innerType->IsTuple()) {
                auto resItems = res.MutableVariant()->MutableTupleItems();
                TTupleType *tupleType = static_cast<TTupleType *>(innerType);
                if (tupleType->GetElementsCount()) {
                    ExportTupleTypeToProto(tupleType, *resItems);
                }
            } else {
                MKQL_ENSURE(false,
                            TStringBuilder() << "Unknown underlying variant type: " << innerType->GetKindAsStr());
            }
            break;
        }

        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
    }
}

void ExportTypeToProtoImpl(TType* type, Ydb::Type& res, const TVector<ui32>* columnOrder = nullptr) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
            res.set_void_type(::google::protobuf::NULL_VALUE);
            break;

        case TType::EKind::Null:
            res.set_null_type(::google::protobuf::NULL_VALUE);
            break;

        case TType::EKind::EmptyList:
            res.set_empty_list_type(::google::protobuf::NULL_VALUE);
            break;

        case TType::EKind::EmptyDict:
            res.set_empty_dict_type(::google::protobuf::NULL_VALUE);
            break;

        case TType::EKind::Data: {
            auto dataType = static_cast<TDataType*>(type);
            auto schemeType = dataType->GetSchemeType();

            switch (schemeType) {
                case NYql::NProto::TypeIds::Decimal: {
                    auto decimalType = static_cast<TDataDecimalType*>(dataType);
                    auto params = decimalType->GetParams();
                    auto decimalParams = res.mutable_decimal_type();
                    decimalParams->set_precision(params.first);
                    decimalParams->set_scale(params.second);
                    break;
                }
                default:
                    ExportPrimitiveTypeToProto(schemeType, res);
                    break;
            }

            break;
        }

        case TType::EKind::Pg: {
            auto* pgType = static_cast<TPgType*>(type);
            auto typeId = pgType->GetTypeId();
            auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(typeId);
            MKQL_ENSURE(typeDesc, TStringBuilder() << "Unknown PG type id: " << typeId);

            auto* pg = res.mutable_pg_type();
            pg->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
            pg->set_oid(typeId);
            pg->set_typmod(-1);
            const i32 typlen = NYql::NPg::LookupType(pgType->GetTypeId()).TypeLen;
            pg->set_typlen(typlen);
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<TOptionalType*>(type);
            ExportTypeToProtoImpl(optionalType->GetItemType(), *res.mutable_optional_type()->mutable_item());
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<TListType*>(type);
            ExportTypeToProtoImpl(listType->GetItemType(), *res.mutable_list_type()->mutable_item());
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType*>(type);
            auto resStruct = res.mutable_struct_type();
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto newMember = resStruct->add_members();
                ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? index : (*columnOrder)[index];
                newMember->set_name(TString(structType->GetMemberName(memberIndex)));
                ExportTypeToProtoImpl(structType->GetMemberType(memberIndex), *newMember->mutable_type());
            }

            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(type);
            auto resTuple = res.mutable_tuple_type();
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                ExportTypeToProtoImpl(tupleType->GetElementType(index), *resTuple->add_elements());
            }

            break;
        }

        case TType::EKind::Dict:  {
            auto dictType = static_cast<TDictType*>(type);
            ExportTypeToProtoImpl(dictType->GetKeyType(), *res.mutable_dict_type()->mutable_key());
            ExportTypeToProtoImpl(dictType->GetPayloadType(), *res.mutable_dict_type()->mutable_payload());
            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<TVariantType*>(type);
            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                TStructType* structType = static_cast<TStructType*>(innerType);
                auto resItems = res.mutable_variant_type()->mutable_struct_items();
                for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                    auto newMember = resItems->add_members();
                    newMember->set_name(TString(structType->GetMemberName(index)));
                    ExportTypeToProtoImpl(structType->GetMemberType(index), *newMember->mutable_type());
                }
            } else if (innerType->IsTuple()) {
                auto resItems = res.mutable_variant_type()->mutable_tuple_items();
                TTupleType* tupleType = static_cast<TTupleType*>(innerType);
                for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                    ExportTypeToProtoImpl(tupleType->GetElementType(index), *resItems->add_elements());
                }
            } else {
                MKQL_ENSURE(false, TStringBuilder() << "Unknown underlying variant type: " << innerType->GetKindAsStr());
            }
            break;
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<TTaggedType*>(type);
            TType* innerType = taggedType->GetBaseType();
            auto& resType = *res.mutable_tagged_type();
            resType.set_tag(TString(taggedType->GetTag()));
            ExportTypeToProtoImpl(innerType, *resType.mutable_type());
            break;
        }

        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
    }
}


Y_FORCE_INLINE void HandleKindDataExport(const TType* type, const NUdf::TUnboxedValuePod& value, NKikimrMiniKQL::TValue& res) {
    auto dataType = static_cast<const TDataType*>(type);
    switch (dataType->GetSchemeType()) {
        case NUdf::TDataType<bool>::Id:
            res.SetBool(value.Get<bool>());
            break;
        case NUdf::TDataType<ui8>::Id:
            res.SetUint32(value.Get<ui8>());
            break;
        case NUdf::TDataType<i8>::Id:
            res.SetInt32(value.Get<i8>());
            break;
        case NUdf::TDataType<ui16>::Id:
            res.SetUint32(value.Get<ui16>());
            break;
        case NUdf::TDataType<i16>::Id:
            res.SetInt32(value.Get<i16>());
            break;
        case NUdf::TDataType<i32>::Id:
        case NUdf::TDataType<NUdf::TDate32>::Id:
            res.SetInt32(value.Get<i32>());
            break;
        case NUdf::TDataType<ui32>::Id:
            res.SetUint32(value.Get<ui32>());
            break;
        case NUdf::TDataType<i64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            res.SetInt64(value.Get<i64>());
            break;
        case NUdf::TDataType<ui64>::Id:
            res.SetUint64(value.Get<ui64>());
            break;
        case NUdf::TDataType<float>::Id:
            res.SetFloat(value.Get<float>());
            break;
        case NUdf::TDataType<double>::Id:
            res.SetDouble(value.Get<double>());
            break;
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id: {
            auto stringRef = value.AsStringRef();
            res.SetText(stringRef.Data(), stringRef.Size());
            break;
        }
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
        case NUdf::TDataType<NUdf::TTzDate32>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            const NUdf::TUnboxedValue out(ValueToString(NUdf::GetDataSlot(dataType->GetSchemeType()), value));
            const auto& stringRef = out.AsStringRef();
            res.SetText(stringRef.Data(), stringRef.Size());
            break;
        }
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            auto decimal = value.GetInt128();
            const auto p = reinterpret_cast<ui8*>(&decimal);
            res.SetLow128(*reinterpret_cast<ui64*>(p));
            res.SetHi128(*reinterpret_cast<ui64*>(p+8));
            break;
        }
        case NUdf::TDataType<NUdf::TDate>::Id:
            res.SetUint32(value.Get<ui16>());
            break;
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            res.SetUint32(value.Get<ui32>());
            break;
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            res.SetUint64(value.Get<ui64>());
            break;
        case NUdf::TDataType<NUdf::TInterval>::Id:
            res.SetInt64(value.Get<i64>());
            break;
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            const auto& stringRef = value.AsStringRef();
            UuidToMkqlProto(stringRef.Data(), stringRef.Size(), res);
            break;
        }
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            auto stringRef = value.AsStringRef();
            res.SetBytes(stringRef.Data(), stringRef.Size());
            break;
        }
        case NUdf::TDataType<NUdf::TDyNumber>::Id: {
            auto stringRef = value.AsStringRef();
            res.SetBytes(stringRef.Data(), stringRef.Size());
            break;
        }
        default:
            auto stringRef = value.AsStringRef();
            res.SetBytes(stringRef.Data(), stringRef.Size());
    }
}

void ExportValueToProtoImpl(TType* type, const NUdf::TUnboxedValuePod& value, NKikimrMiniKQL::TValue& res, const TVector<ui32>* columnOrder = nullptr) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            break;

        case TType::EKind::Null: {
            res.SetNullFlagValue(::google::protobuf::NULL_VALUE);
            break;
        }

        case TType::EKind::Data: {
            HandleKindDataExport(type, value, res);
            break;
        }

        case TType::EKind::Pg: {
            if (!value) {
                res.SetNullFlagValue(::google::protobuf::NULL_VALUE);
                break;
            }
            auto pgType = static_cast<TPgType*>(type);
            auto textValue = NYql::NCommon::PgValueToNativeText(value, pgType->GetTypeId());
            res.SetText(textValue);
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<TOptionalType*>(type);
            if (value) {
                ExportValueToProtoImpl(optionalType->GetItemType(), value.GetOptionalValue(), *res.MutableOptional());
            }

            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<TListType*>(type);
            auto itemType = listType->GetItemType();
            if (value.HasFastListLength())
                res.MutableList()->Reserve(value.GetListLength());
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                ExportValueToProtoImpl(itemType, item, *res.MutableList()->Add());
            }

            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType*>(type);
            res.MutableStruct()->Reserve(structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberIndex = (!columnOrder || columnOrder->empty()) ?  index : (*columnOrder)[index];
                auto memberType = structType->GetMemberType(memberIndex);
                ExportValueToProtoImpl(memberType, value.GetElement(memberIndex), *res.MutableStruct()->Add());
            }

            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(type);
            res.MutableTuple()->Reserve(tupleType->GetElementsCount());
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                ExportValueToProtoImpl(elementType, value.GetElement(index), *res.MutableTuple()->Add());
            }

            break;
        }

        case TType::EKind::Dict:  {
            auto dictType = static_cast<TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            const auto iter = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                auto dictItem = res.MutableDict()->Add();
                ExportValueToProtoImpl(keyType, key, *dictItem->MutableKey());
                ExportValueToProtoImpl(payloadType, payload, *dictItem->MutablePayload());
            }

            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<TVariantType*>(type);
            auto variantIndex = value.GetVariantIndex();
            res.SetVariantIndex(variantIndex);
            ExportValueToProtoImpl(variantType->GetAlternativeType(variantIndex), value.GetVariantItem(), *res.MutableOptional());

            break;
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<TTaggedType*>(type);
            ExportValueToProtoImpl(taggedType->GetBaseType(), value, res);
            break;
        }

        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
    }
}

void ExportValueToProtoImpl(TType* type, const NUdf::TUnboxedValuePod& value, Ydb::Value& res, const TVector<ui32>* columnOrder = nullptr) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
        break;

        case TType::EKind::Null: {
            res.set_null_flag_value(::google::protobuf::NULL_VALUE);
            break;
        }

        case TType::EKind::Data: {
            HandleKindDataExport(type, value, res);
            break;
        }

        case TType::EKind::Pg: {
            if (!value) {
                res.set_null_flag_value(::google::protobuf::NULL_VALUE);
                break;
            }
            auto pgType = static_cast<TPgType*>(type);
            auto textValue = NYql::NCommon::PgValueToNativeText(value, pgType->GetTypeId());
            res.set_text_value(textValue);
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<TOptionalType*>(type);
            if (value.HasValue()) {
                const auto nextType = optionalType->GetItemType();
                if (nextType->GetKind() == TType::EKind::Data) {
                    HandleKindDataExport(nextType, value.GetOptionalValue(), res);
                } else {
                    ExportValueToProtoImpl(nextType, value.GetOptionalValue(), res);
                }
            } else {
                if (value) {
                    ExportValueToProtoImpl(optionalType->GetItemType(), value.GetOptionalValue(), *res.mutable_nested_value());
                } else {
                    res.set_null_flag_value(::google::protobuf::NULL_VALUE);
                }
            }

            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<TListType*>(type);
            auto itemType = listType->GetItemType();
            if (value.HasFastListLength()) {
                res.mutable_items()->Reserve(value.GetListLength());
            }
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                ExportValueToProtoImpl(itemType, item, *res.mutable_items()->Add());
            }

            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<TStructType*>(type);
            res.mutable_items()->Reserve(structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberIndex = (!columnOrder || columnOrder->empty()) ?  index : (*columnOrder)[index];
                auto memberType = structType->GetMemberType(memberIndex);
                ExportValueToProtoImpl(memberType, value.GetElement(memberIndex), *res.mutable_items()->Add());
            }

            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(type);
            res.mutable_items()->Reserve(tupleType->GetElementsCount());
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                ExportValueToProtoImpl(elementType, value.GetElement(index), *res.mutable_items()->Add());
            }

            break;
        }

        case TType::EKind::Dict:  {
            auto dictType = static_cast<TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            const auto iter = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                auto dictItem = res.mutable_pairs()->Add();
                ExportValueToProtoImpl(keyType, key, *dictItem->mutable_key());
                ExportValueToProtoImpl(payloadType, payload, *dictItem->mutable_payload());
            }

            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<TVariantType*>(type);
            auto variantIndex = value.GetVariantIndex();
            res.set_variant_index(variantIndex);
            ExportValueToProtoImpl(variantType->GetAlternativeType(variantIndex), value.GetVariantItem(), *res.mutable_nested_value());

            break;
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<TTaggedType*>(type);
            ExportValueToProtoImpl(taggedType->GetBaseType(), value, res);
            break;
        }

        default: {
            MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
        }
    }
}

Y_FORCE_INLINE NUdf::TUnboxedValue HandleKindDataImport(const TType* type, const NKikimrMiniKQL::TValue& value) {
    auto dataType = static_cast<const TDataType*>(type);
    auto oneOfCase = value.value_value_case();
    switch (dataType->GetSchemeType()) {
        case NUdf::TDataType<bool>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kBool);
            return NUdf::TUnboxedValuePod(value.GetBool());
        case NUdf::TDataType<ui8>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint32);
            return NUdf::TUnboxedValuePod(ui8(value.GetUint32()));
        case NUdf::TDataType<i8>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kInt32);
            return NUdf::TUnboxedValuePod(i8(value.GetInt32()));
        case NUdf::TDataType<ui16>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint32);
            return NUdf::TUnboxedValuePod(ui16(value.GetUint32()));
        case NUdf::TDataType<i16>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kInt32);
            return NUdf::TUnboxedValuePod(i16(value.GetInt32()));
        case NUdf::TDataType<i32>::Id:
        case NUdf::TDataType<NUdf::TDate32>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kInt32);
            return NUdf::TUnboxedValuePod(value.GetInt32());
        case NUdf::TDataType<ui32>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint32);
            return NUdf::TUnboxedValuePod(value.GetUint32());
        case NUdf::TDataType<i64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kInt64);
            return NUdf::TUnboxedValuePod(value.GetInt64());
        case NUdf::TDataType<ui64>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint64);
            return NUdf::TUnboxedValuePod(value.GetUint64());
        case NUdf::TDataType<float>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kFloat);
            return NUdf::TUnboxedValuePod(value.GetFloat());
        case NUdf::TDataType<double>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kDouble);
            return NUdf::TUnboxedValuePod(value.GetDouble());
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kText);
            return MakeString(value.GetText());
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
        case NUdf::TDataType<NUdf::TTzDate32>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kText);
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.GetText()));
        }
        case NUdf::TDataType<NUdf::TDate>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint32);
            return NUdf::TUnboxedValuePod(ui16(value.GetUint32()));
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint32);
            return NUdf::TUnboxedValuePod(value.GetUint32());
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kUint64);
            return NUdf::TUnboxedValuePod(value.GetUint64());
        case NUdf::TDataType<NUdf::TInterval>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kInt64);
            return NUdf::TUnboxedValuePod(value.GetInt64());
        case NUdf::TDataType<NUdf::TJsonDocument>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kBytes);
            return MakeString(value.GetBytes());
        case NUdf::TDataType<NUdf::TDecimal>::Id:
            return NUdf::TUnboxedValuePod(NYql::NDecimal::FromHalfs(value.GetLow128(), value.GetHi128()));
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kBytes);
            return MakeString(value.GetBytes());
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kLow128);
            union {
                ui64 half[2];
                char bytes[16];
            } buf;
            buf.half[0] = value.GetLow128();
            buf.half[1] = value.GetHi128();
            return MakeString(NUdf::TStringRef(buf.bytes, 16));
        }
        default:
            MKQL_ENSURE_S(oneOfCase == NKikimrMiniKQL::TValue::ValueValueCase::kBytes,
                "got: " << (int) oneOfCase << ", type: " << (int) dataType->GetSchemeType());
            return MakeString(value.GetBytes());
    }
}

}

void ExportPrimitiveTypeToProto(ui32 schemeType, Ydb::Type& output) {
    switch (schemeType) {
        case NYql::NProto::TypeIds::Bool:
        case NYql::NProto::TypeIds::Int8:
        case NYql::NProto::TypeIds::Uint8:
        case NYql::NProto::TypeIds::Int16:
        case NYql::NProto::TypeIds::Uint16:
        case NYql::NProto::TypeIds::Int32:
        case NYql::NProto::TypeIds::Uint32:
        case NYql::NProto::TypeIds::Int64:
        case NYql::NProto::TypeIds::Uint64:
        case NYql::NProto::TypeIds::Float:
        case NYql::NProto::TypeIds::Double:
        case NYql::NProto::TypeIds::Date:
        case NYql::NProto::TypeIds::Datetime:
        case NYql::NProto::TypeIds::Timestamp:
        case NYql::NProto::TypeIds::Interval:
        case NYql::NProto::TypeIds::TzDate:
        case NYql::NProto::TypeIds::TzDatetime:
        case NYql::NProto::TypeIds::TzTimestamp:
        case NYql::NProto::TypeIds::String:
        case NYql::NProto::TypeIds::Utf8:
        case NYql::NProto::TypeIds::Yson:
        case NYql::NProto::TypeIds::Json:
        case NYql::NProto::TypeIds::Uuid:
        case NYql::NProto::TypeIds::JsonDocument:
        case NYql::NProto::TypeIds::DyNumber:
        case NYql::NProto::TypeIds::Date32:
        case NYql::NProto::TypeIds::Datetime64:
        case NYql::NProto::TypeIds::Timestamp64:
        case NYql::NProto::TypeIds::Interval64:
        case NYql::NProto::TypeIds::TzDate32:
        case NYql::NProto::TypeIds::TzDatetime64:
        case NYql::NProto::TypeIds::TzTimestamp64:
            output.set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(schemeType));
            break;

        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unsupported primitive scheme type: " << schemeType);
    }
}

void ExportTypeToProto(TType* type, Ydb::Type& res, const TVector<ui32>* columnOrder) {
    ExportTypeToProtoImpl(type, res, columnOrder);
}

void ExportValueToProto(TType* type, const NUdf::TUnboxedValuePod& value, Ydb::Value& res, const TVector<ui32>* columnOrder) {
    ExportValueToProtoImpl(type, value, res, columnOrder);
}

template <typename ValueType> class TBufferArray {
protected:
    using size_type = ui32;
    using cookie_type = decltype(((TStructType*)nullptr)->GetCookie());

    struct header_type {
        size_type size;
        size_type capacity;
    };

    void* Buffer;

    explicit TBufferArray(size_type capacity, void* buffer)
            : Buffer(buffer)
    {
        static_assert(sizeof(cookie_type) == sizeof(Buffer), "cookie_type should be equal to uintptr_t");
        new (buffer) header_type({0, capacity});
    }

public:
    TBufferArray(void* buffer)
            : Buffer(buffer)
    {}

    void* buffer() {
        return Buffer;
    }

    const void* buffer() const {
        return Buffer;
    }

    header_type* header() {
        return reinterpret_cast<header_type*>(buffer());
    }

    const header_type* header() const {
        return reinterpret_cast<const header_type*>(buffer());
    }

    ValueType* data() {
        return reinterpret_cast<ValueType*>((reinterpret_cast<char*>(buffer()) + sizeof(header_type)));
    }

    const ValueType* data() const {
        return reinterpret_cast<const ValueType*>((reinterpret_cast<char*>(buffer()) + sizeof(header_type)));
    }

    size_type size() const {
        return header()->size;
    }

    size_type capacity() const {
        return header()->capacity;
    }

    bool empty() const {
        return buffer() == nullptr || size() == 0;
    }

    ValueType* begin() {
        return data();
    }

    ValueType* end() {
        return begin() + size();
    }

    void push_back(const ValueType& value) {
        MKQL_ENSURE(size() < capacity(), "not enough size of array");
        new (data() + header()->size) ValueType(value);
        ++(header()->size);
    }

    template <typename... Types> void emplace_back(Types... types) {
        MKQL_ENSURE(size() < capacity(), "not enough size of array");
        new (data() + header()->size) ValueType(types...);
        ++(header()->size);
    }

    const ValueType& operator [] (size_type pos) const {
        MKQL_ENSURE(pos < size(), "out of range");
        return data()[pos];
    }

    ValueType& operator [] (size_type pos) {
        MKQL_ENSURE(pos < size(), "out of range");
        return data()[pos];
    }

    static ui64 GetRequiredSize(size_type count) {
        return sizeof(header_type) + sizeof(ValueType) * count;
    }

    static TBufferArray Create(size_type count, const TTypeEnvironment& env) {
        void* buffer = env.AllocateBuffer(GetRequiredSize(count));
        return TBufferArray(count, buffer);
    }

    cookie_type ToCookie() const {
        return reinterpret_cast<cookie_type>(Buffer);
    }

    static TBufferArray FromCookie(cookie_type cookie) {
        return TBufferArray(reinterpret_cast<void*>(cookie));
    }
};

using TRemapArray = TBufferArray<ui32>;

class TProtoImporter {
public:
    TProtoImporter(const TTypeEnvironment& env);
    ~TProtoImporter();
    TType* ImportTypeFromProto(const NKikimrMiniKQL::TType& type);
    TType* ImportTypeFromProto(const Ydb::Type& type);
    TNode* ImportNodeFromProto(TType* type, const NKikimrMiniKQL::TValue& value);
    NUdf::TUnboxedValue ImportValueFromProto(const TType* type, const Ydb::Value& value,
        const THolderFactory& factory);
    NUdf::TUnboxedValue ImportValueFromProto(const TType* type, const NKikimrMiniKQL::TValue& value,
        const THolderFactory& factory);
private:
    TTupleType* ImportTupleTypeFromProto(const Ydb::TupleType& protoTupleType);
    TStructType* ImportStructTypeFromProto(const Ydb::StructType& protoStructType);
    TTupleType* ImportTupleTypeFromProto(const NKikimrMiniKQL::TTupleType& protoTupleType);
    TStructType* ImportStructTypeFromProto(const NKikimrMiniKQL::TStructType& protoStructType);
    const TTypeEnvironment& env;
    TStackVec<TStructType*, 16> SortedStructs;
};

TProtoImporter::TProtoImporter(const TTypeEnvironment& env)
        : env(env)
{}

TProtoImporter::~TProtoImporter() {
    for (TStructType* structType : SortedStructs) {
        structType->SetCookie(0);
    }
}

TTupleType* TProtoImporter::ImportTupleTypeFromProto(const NKikimrMiniKQL::TTupleType& protoTupleType)
{
    const ui32 elementsCount = static_cast<ui32>(protoTupleType.ElementSize());
    TSmallVec<TType*> elementTypes;
    for (ui32 elementIdx = 0; elementIdx < elementsCount; ++elementIdx) {
        const NKikimrMiniKQL::TType& elementProtoType = protoTupleType.GetElement(elementIdx);
        TType* elementType = ImportTypeFromProto(elementProtoType);
        elementTypes.emplace_back(elementType);
    }

    return TTupleType::Create(elementsCount, elementTypes.data(), env);
}

TStructType* TProtoImporter::ImportStructTypeFromProto(const NKikimrMiniKQL::TStructType& protoStructType)
{
    ui32 membersCount = static_cast<ui32>(protoStructType.MemberSize());
    TStackVec<std::pair<TString, TType*>, 16> members;
    TRemapArray remap = TRemapArray::Create(membersCount, env);
    members.reserve(membersCount);
    for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
        const NKikimrMiniKQL::TMember& protoMember = protoStructType.GetMember(memberNum);
        TType* child = ImportTypeFromProto(protoMember.GetType());
        members.emplace_back(protoMember.GetName(), child);
        remap.emplace_back(memberNum);
    }
    Sort(remap, [&members](ui32 a, ui32 b) -> bool { return members[a].first < members[b].first; });
    TStackVec<std::pair<TString, TType*>, 16> sortedMembers(membersCount);
    bool resorted = false;
    for (ui32 pos = 0; pos < membersCount; ++pos) {
        sortedMembers[pos] = members[remap[pos]];
        if (remap[pos] != pos)
            resorted = true;
    }
    TStructType* structType = TStructType::Create(sortedMembers.data(), sortedMembers.size(), env);
    if (resorted) {
        structType->SetCookie(remap.ToCookie());
        SortedStructs.push_back(structType);
    }
    return structType;
}

TType* TProtoImporter::ImportTypeFromProto(const NKikimrMiniKQL::TType& type) {
    switch (type.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void: {
            return env.GetVoidLazy()->GetType();
        }
        case NKikimrMiniKQL::ETypeKind::Null: {
            return env.GetNullLazy()->GetType();
        }
        case NKikimrMiniKQL::ETypeKind::Data: {
            const NKikimrMiniKQL::TDataType& protoData = type.GetData();
            NUdf::TDataTypeId schemeType = protoData.GetScheme();
            MKQL_ENSURE(NUdf::FindDataSlot(schemeType), TStringBuilder() << "unknown type id: " << schemeType);
            if (schemeType == NYql::NProto::TypeIds::Decimal) {
                const NKikimrMiniKQL::TDecimalParams& decimalParams = protoData.GetDecimalParams();
                return TDataDecimalType::Create(decimalParams.GetPrecision(), decimalParams.GetScale(), env);
            } else {
                return TDataType::Create(schemeType, env);
            }
        }
        case NKikimrMiniKQL::ETypeKind::Pg: {
            const NKikimrMiniKQL::TPgType& protoPgType = type.GetPg();
            return TPgType::Create(protoPgType.Getoid(), env);
        }
        case NKikimrMiniKQL::ETypeKind::Optional: {
            const NKikimrMiniKQL::TOptionalType& protoOptionalType = type.GetOptional();
            TType* child = ImportTypeFromProto(protoOptionalType.GetItem());
            TOptionalType* optionalType = TOptionalType::Create(child, env);
            return optionalType;
        }
        case NKikimrMiniKQL::ETypeKind::Tagged: {
            const NKikimrMiniKQL::TTaggedType& protoTaggedType = type.GetTagged();
            TType* child = ImportTypeFromProto(protoTaggedType.GetItem());
            return TTaggedType::Create(child, protoTaggedType.GetTag(), env);
        }
        case NKikimrMiniKQL::ETypeKind::EmptyList: {
            return env.GetTypeOfEmptyListLazy();
        }
        case NKikimrMiniKQL::ETypeKind::List: {
            const NKikimrMiniKQL::TListType& protoListType = type.GetList();
            const NKikimrMiniKQL::TType& protoItemType = protoListType.GetItem();
            TType* itemType = ImportTypeFromProto(protoItemType);
            TListType* listType = TListType::Create(itemType, env);
            return listType;
        }
        case NKikimrMiniKQL::ETypeKind::Tuple: {
            const NKikimrMiniKQL::TTupleType& protoTupleType = type.GetTuple();
            return ImportTupleTypeFromProto(protoTupleType);
        }
        case NKikimrMiniKQL::ETypeKind::Struct: {
            const NKikimrMiniKQL::TStructType& protoStructType = type.GetStruct();
            return ImportStructTypeFromProto(protoStructType);
        }
        case NKikimrMiniKQL::ETypeKind::EmptyDict: {
            return env.GetTypeOfEmptyDictLazy();
        }
        case NKikimrMiniKQL::ETypeKind::Dict: {
            const NKikimrMiniKQL::TDictType& protoDictType = type.GetDict();

            TType* keyType = ImportTypeFromProto(protoDictType.GetKey());
            TType* payloadType = ImportTypeFromProto(protoDictType.GetPayload());

            TDictType* dictType = TDictType::Create(keyType, payloadType, env);
            return dictType;
        }
        case NKikimrMiniKQL::ETypeKind::Variant: {
            const NKikimrMiniKQL::TVariantType& protoVariantType = type.GetVariant();
            switch (protoVariantType.type_case()) {
                case NKikimrMiniKQL::TVariantType::kTupleItems: {
                    const NKikimrMiniKQL::TTupleType& protoTupleType = protoVariantType.GetTupleItems();

                    TTupleType* tupleType = ImportTupleTypeFromProto(protoTupleType);

                    return TVariantType::Create(tupleType, env);
                }
                case NKikimrMiniKQL::TVariantType::kStructItems: {
                    const NKikimrMiniKQL::TStructType& protoStructType = protoVariantType.GetStructItems();

                    TStructType* structType = ImportStructTypeFromProto(protoStructType);

                    return TVariantType::Create(structType, env);
                }
                default:
                    MKQL_ENSURE(false, TStringBuilder() << "Unknown variant type representation: " << protoVariantType.DebugString());
            }
        }
        default: {
            MKQL_ENSURE(false, TStringBuilder() << "Unknown protobuf type: " << type.DebugString());
        }
    }
}

TNode* TProtoImporter::ImportNodeFromProto(TType* type, const NKikimrMiniKQL::TValue& value) {
    switch (type->GetKind()) {
        case TType::EKind::Void: {
            return env.GetVoidLazy();
        }
        case TType::EKind::Null: {
            return env.GetNullLazy();
        }
        case TType::EKind::Data: {
            TDataType* dataType = static_cast<TDataType*>(type);
            TDataLiteral* dataNode = nullptr;
            switch (const auto schemeType = dataType->GetSchemeType()) {
                case NUdf::TDataType<bool>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetBool()), dataType, env);
                    break;
                case NUdf::TDataType<i8>::Id: {
                    auto dataValue = value.GetInt32();
                    MKQL_ENSURE(dataValue <= Max<i8>() && dataValue >= Min<i8>(),
                        TStringBuilder() << "Cannot cast value " << dataValue << " to int8");
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(i8(dataValue)), dataType, env);
                    break;
                }
                case NUdf::TDataType<ui8>::Id: {
                    auto dataValue = value.GetUint32();
                    MKQL_ENSURE(dataValue <= Max<ui8>(), TStringBuilder() << "Cannot cast value " << dataValue << " to uint8");
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(ui8(dataValue)), dataType, env);
                    break;
                }
                case NUdf::TDataType<i16>::Id: {
                    auto dataValue = value.GetInt32();
                    MKQL_ENSURE(dataValue <= Max<i16>() && dataValue >= Min<i16>(),
                        TStringBuilder() << "Cannot cast value " << dataValue << " to int16");
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(i16(dataValue)), dataType, env);
                    break;
                }
                case NUdf::TDataType<ui16>::Id: {
                    auto dataValue = value.GetUint32();
                    MKQL_ENSURE(dataValue <= Max<ui16>(), TStringBuilder() << "Cannot cast value " << dataValue << " to uint16");
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(ui16(dataValue)), dataType, env);
                    break;
                }
                case NUdf::TDataType<i32>::Id:
                case NUdf::TDataType<NUdf::TDate32>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetInt32()), dataType, env);
                    break;
                case NUdf::TDataType<ui32>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetUint32()), dataType, env);
                    break;
                case NUdf::TDataType<i64>::Id:
                case NUdf::TDataType<NUdf::TDatetime64>::Id:
                case NUdf::TDataType<NUdf::TTimestamp64>::Id:
                case NUdf::TDataType<NUdf::TInterval64>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetInt64()), dataType, env);
                    break;
                case NUdf::TDataType<ui64>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetUint64()), dataType, env);
                    break;
                case NUdf::TDataType<float>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetFloat()), dataType, env);
                    break;
                case NUdf::TDataType<double>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetDouble()), dataType, env);
                    break;
                case NUdf::TDataType<char*>::Id:
                case NUdf::TDataType<NUdf::TYson>::Id:
                case NUdf::TDataType<NUdf::TJsonDocument>::Id:
                case NUdf::TDataType<NUdf::TDyNumber>::Id:
                    dataNode = TDataLiteral::Create(env.NewStringValue(value.GetBytes()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TJson>::Id:
                case NUdf::TDataType<NUdf::TUtf8>::Id:
                    dataNode = TDataLiteral::Create(env.NewStringValue(value.GetText()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TTzDate>::Id:
                case NUdf::TDataType<NUdf::TTzDatetime>::Id:
                case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
                case NUdf::TDataType<NUdf::TTzDate32>::Id:
                case NUdf::TDataType<NUdf::TTzDatetime64>::Id:
                case NUdf::TDataType<NUdf::TTzTimestamp64>::Id:
                    dataNode = TDataLiteral::Create(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.GetText()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TDate>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(ui16(value.GetUint32())), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TDatetime>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetUint32()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TTimestamp>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetUint64()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TInterval>::Id:
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(value.GetInt64()), dataType, env);
                    break;
                case NUdf::TDataType<NUdf::TDecimal>::Id: {
                    using NYql::NDecimal::FromProto;
                    dataNode = TDataLiteral::Create(NUdf::TUnboxedValuePod(FromProto(value)), dataType, env);
                    break;
                }
                case NUdf::TDataType<NUdf::TUuid>::Id: {
                    union {
                        ui64 half[2];
                        char bytes[16];
                    } buf;
                    buf.half[0] = value.GetLow128();
                    buf.half[1] = value.GetHi128();
                    dataNode = TDataLiteral::Create(env.NewStringValue(NUdf::TStringRef(buf.bytes, 16)), dataType, env);
                    break;
                }
                default:
                    MKQL_ENSURE(false, TStringBuilder() << "Unknown data type: " << schemeType);
            }
            return dataNode;
        }
        case TType::EKind::Optional: {
            TOptionalType* optionalType = static_cast<TOptionalType*>(type);
            TOptionalLiteral* optionalNode;
            if (value.HasOptional()) {
                const NKikimrMiniKQL::TValue& protoValue = value.GetOptional();
                TNode* child = ImportNodeFromProto(optionalType->GetItemType(), protoValue);
                optionalNode = TOptionalLiteral::Create(TRuntimeNode(child, true), optionalType, env);
            } else {
                optionalNode = TOptionalLiteral::Create(optionalType, env);
            }
            return optionalNode;
        }
        case TType::EKind::List: {
            TListType* listType = static_cast<TListType*>(type);
            TType* itemType = listType->GetItemType();
            TVector<TRuntimeNode> items;
            ui32 listSize = value.ListSize();
            items.reserve(listSize);
            for (ui32 itemNum = 0; itemNum < listSize; ++itemNum) {
                const NKikimrMiniKQL::TValue& protoValue = value.GetList(itemNum);
                TNode* item = ImportNodeFromProto(itemType, protoValue);
                items.push_back(TRuntimeNode(item, true));
            }
            TListLiteral* listNode = TListLiteral::Create(items.data(), items.size(), listType, env);
            return listNode;
        }
        case TType::EKind::Tuple: {
            TTupleType* tupleType = static_cast<TTupleType*>(type);
            ui32 elementsCount = tupleType->GetElementsCount();
            MKQL_ENSURE(elementsCount == value.TupleSize(), "Invalid protobuf format, tuple size mismatch between Type and Value");
            TSmallVec<TRuntimeNode> elements(elementsCount);
            for (ui32 elementIdx = 0; elementIdx < elementsCount; ++elementIdx) {
                TType* elementType = tupleType->GetElementType(elementIdx);
                auto& elementProtoValue = value.GetTuple(elementIdx);
                TNode* elementNode = ImportNodeFromProto(elementType, elementProtoValue);
                elements[elementIdx] = TRuntimeNode(elementNode, true);
            }
            TTupleLiteral* tupleNode = TTupleLiteral::Create(elements.size(), elements.data(), tupleType, env);
            return tupleNode;
        }
        case TType::EKind::Struct: {
            TStructType* structType = static_cast<TStructType*>(type);
            ui32 membersCount = structType->GetMembersCount();
            MKQL_ENSURE(membersCount == value.StructSize(), "Invalid protobuf format, struct size mismatch between Type and Value");
            TStackVec<TRuntimeNode, 16> members(membersCount);
            TRemapArray remap = TRemapArray::FromCookie(structType->GetCookie());
            for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
                ui32 sortedMemberNum = remap.empty() ? memberNum : remap[memberNum];
                TType* memberType = structType->GetMemberType(memberNum);
                const NKikimrMiniKQL::TValue& protoValue = value.GetStruct(sortedMemberNum);
                TNode* child = ImportNodeFromProto(memberType, protoValue);
                members[memberNum] = TRuntimeNode(child, true);
            }
            TStructLiteral* structNode = TStructLiteral::Create(members.size(), members.data(), structType, env);
            return structNode;
        }
        case TType::EKind::Dict: {
            TDictType* dictType = static_cast<TDictType*>(type);
            ui32 dictSize = value.DictSize();

            TVector<std::pair<TRuntimeNode, TRuntimeNode>> items;
            for (ui32 itemNum = 0; itemNum < dictSize; ++itemNum) {
                const NKikimrMiniKQL::TValuePair& protoPair = value.GetDict(itemNum);
                TNode* keyNode = ImportNodeFromProto(dictType->GetKeyType(), protoPair.GetKey());
                TNode* payloadNode = ImportNodeFromProto(dictType->GetPayloadType(), protoPair.GetPayload());
                items.push_back(std::make_pair(TRuntimeNode(keyNode, true), TRuntimeNode(payloadNode, true)));
            }

            TDictLiteral* dictNode = TDictLiteral::Create(items.size(), items.data(), dictType, env);
            return dictNode;
        }
        case TType::EKind::Variant: {
            TVariantType* variantType = static_cast<TVariantType*>(type);
            auto variantIndex = value.GetVariantIndex();
            TType* innerType = variantType->GetAlternativeType(variantIndex);
            TNode* node = ImportNodeFromProto(innerType, value.GetOptional());
            return TVariantLiteral::Create(TRuntimeNode(node, true), variantIndex, variantType, env);
        }
        default:
            break;
    }
    MKQL_ENSURE(false, TStringBuilder() << "Unknown protobuf type: " << type->GetKindAsStr());
}

void ExportTypeToProto(TType* type, NKikimrMiniKQL::TType& res, const TVector<ui32>* columnOrder) {
    ExportTypeToProtoImpl(type, res, columnOrder);
}

void ExportValueToProto(TType* type, const NUdf::TUnboxedValuePod& value, NKikimrMiniKQL::TValue& res, const TVector<ui32>* columnOrder) {
    ExportValueToProtoImpl(type, value, res, columnOrder);
}

TTupleType* TProtoImporter::ImportTupleTypeFromProto(const Ydb::TupleType& input) {
    const ui32 elementsCount = input.elementsSize();
    TSmallVec<TType*> elementTypes;
    for(ui32 idx = 0; idx < elementsCount; ++idx) {
        elementTypes.emplace_back(ImportTypeFromProto(input.elements(idx)));
    }
    return TTupleType::Create(elementsCount, elementTypes.data(), env);
}

TStructType* TProtoImporter::ImportStructTypeFromProto(const Ydb::StructType& protoStructType) {
    ui32 membersCount = static_cast<ui32>(protoStructType.membersSize());
    TStackVec<std::pair<TString, TType*>, 16> members;
    TRemapArray remap = TRemapArray::Create(membersCount, env);
    members.reserve(membersCount);
    for (ui32 memberNum = 0; memberNum < membersCount; ++memberNum) {
        const Ydb::StructMember& protoMember = protoStructType.members(memberNum);
        TType* child = ImportTypeFromProto(protoMember.type());
        members.emplace_back(protoMember.name(), child);
        remap.emplace_back(memberNum);
    }
    Sort(remap, [&members](ui32 a, ui32 b) -> bool { return members[a].first < members[b].first; });
    TStackVec<std::pair<TString, TType*>, 16> sortedMembers(membersCount);
    bool resorted = false;
    for (ui32 pos = 0; pos < membersCount; ++pos) {
        sortedMembers[pos] = members[remap[pos]];
        if (remap[pos] != pos)
            resorted = true;
    }
    TStructType* structType = TStructType::Create(sortedMembers.data(), sortedMembers.size(), env);
    if (resorted) {
        structType->SetCookie(remap.ToCookie());
        SortedStructs.emplace_back(structType);
    }
    return structType;
}

TType* TProtoImporter::ImportTypeFromProto(const Ydb::Type& input) {
    switch (input.type_case()) {
        case Ydb::Type::kVoidType:
            return env.GetTypeOfVoidLazy();
        case Ydb::Type::kNullType:
            return env.GetTypeOfNullLazy();
        case Ydb::Type::kEmptyListType:
            return env.GetTypeOfEmptyListLazy();
        case Ydb::Type::kEmptyDictType:
            return env.GetTypeOfEmptyDictLazy();
        case Ydb::Type::kPgType: {
            if (const auto& typeName = input.pg_type().type_name()) {
                auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeName(typeName);
                MKQL_ENSURE(typeDesc, TStringBuilder() << "Unknown PG type name: " << typeName);
                return TPgType::Create(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc), env);
            } else {
                const auto& typeId = input.pg_type().oid();
                return TPgType::Create(typeId, env);
            }
        }
        case Ydb::Type::kTypeId: {
            MKQL_ENSURE(NUdf::FindDataSlot(input.type_id()), TStringBuilder() << "unknown type id: " << ui32(input.type_id()));
            return TDataType::Create(input.type_id(), env);
        }
        case Ydb::Type::kDecimalType: {
            return TDataDecimalType::Create(input.decimal_type().precision(), input.decimal_type().scale(), env);
        }
        case Ydb::Type::kOptionalType: {
            TType* underlying = ImportTypeFromProto(input.optional_type().item());
            return TOptionalType::Create(underlying, env);
        }
        case Ydb::Type::kListType: {
            TType* itemType = ImportTypeFromProto(input.list_type().item());
            return TListType::Create(itemType, env);
        }
        case Ydb::Type::kTupleType: {
            return ImportTupleTypeFromProto(input.tuple_type());
        }
        case Ydb::Type::kStructType: {
            return ImportStructTypeFromProto(input.struct_type());
        }
        case Ydb::Type::kDictType: {
            TType* keyType = ImportTypeFromProto(input.dict_type().key());
            TType* payloadType = ImportTypeFromProto(input.dict_type().payload());
            return TDictType::Create(keyType, payloadType, env);
        }
        case Ydb::Type::kVariantType: {
            const Ydb::VariantType& protoVariantType = input.variant_type();
            switch (protoVariantType.type_case()) {
                case Ydb::VariantType::kTupleItems: {
                    return TVariantType::Create(ImportTupleTypeFromProto(protoVariantType.tuple_items()), env);
                }
                case Ydb::VariantType::kStructItems: {
                    return TVariantType::Create(ImportStructTypeFromProto(protoVariantType.struct_items()), env);
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

Y_FORCE_INLINE void CheckTypeId(i32 id, i32 expected, std::string_view typeName) {
    if (id != expected) {
        throw yexception() << "Invalid value representation for type: " << typeName
            << ", expected value case: " << expected << ", but current: " << id;
    }
}

Y_FORCE_INLINE NUdf::TUnboxedValue KindDataImport(const TType* type, const Ydb::Value& value) {
    const TDataType* dataType = static_cast<const TDataType*>(type);
    switch (dataType->GetSchemeType()) {
        case NUdf::TDataType<bool>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kBoolValue, "Bool");
            return NUdf::TUnboxedValuePod(value.bool_value());
        }
        case NUdf::TDataType<ui8>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint8");
            return NUdf::TUnboxedValuePod(ui8(value.uint32_value()));
        }
        case NUdf::TDataType<i8>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int8");
            return NUdf::TUnboxedValuePod(i8(value.int32_value()));
        }
        case NUdf::TDataType<ui16>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint16");
            return NUdf::TUnboxedValuePod(ui16(value.uint32_value()));
        }
        case NUdf::TDataType<i16>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int16");
            return NUdf::TUnboxedValuePod(i16(value.int32_value()));
        }
        case NUdf::TDataType<i32>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt32Value, "Int32");
            return NUdf::TUnboxedValuePod(value.int32_value());
        }
        case NUdf::TDataType<ui32>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Uint32");
            return NUdf::TUnboxedValuePod(value.uint32_value());
        }
        case NUdf::TDataType<i64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Int64");
            return NUdf::TUnboxedValuePod(value.int64_value());
        }
        case NUdf::TDataType<ui64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint64Value, "Uint64");
            return NUdf::TUnboxedValuePod(value.uint64_value());
        }
        case NUdf::TDataType<float>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kFloatValue, "Float");
            return NUdf::TUnboxedValuePod(value.float_value());
        }
        case NUdf::TDataType<double>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kDoubleValue, "Double");
            return NUdf::TUnboxedValuePod(value.double_value());
        }
        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDate");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }
        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDatetime");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzTimestamp");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }
        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDate32");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }
        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzDatetime64");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }
        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "TzTimestamp64");
            return NUdf::TUnboxedValuePod(ValueFromString(NUdf::GetDataSlot(dataType->GetSchemeType()), value.text_value()));
        }        
        case NUdf::TDataType<NUdf::TJson>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "Json");
            const auto& stringRef = value.text_value();
            if (!NYql::NDom::IsValidJson(stringRef)) {
                throw yexception() << "Invalid Json value";
            }
            return MakeString(value.text_value());
        }
        case NUdf::TDataType<NUdf::TUtf8>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "Utf8");
            const auto& stringRef = value.text_value();
            if (!NYql::IsUtf8(stringRef)) {
                throw yexception() << "Invalid Utf8 value";
            }
            return MakeString(value.text_value());
        }
        case NUdf::TDataType<NUdf::TDate>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Date");
            if (value.uint32_value() >= NUdf::MAX_DATE) {
                throw yexception() << "Invalid Date value";
            }
            return NUdf::TUnboxedValuePod(ui16(value.uint32_value()));
        }
        case NUdf::TDataType<NUdf::TDatetime>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Datetime");
            if (value.uint32_value() >= NUdf::MAX_DATETIME) {
                throw yexception() << "Invalid Datetime value";
            }
            return NUdf::TUnboxedValuePod(value.uint32_value());
        }
        case NUdf::TDataType<NUdf::TTimestamp>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint64Value, "Timestamp");
            if (value.uint64_value() >= NUdf::MAX_TIMESTAMP) {
                throw yexception() << "Invalid Timestamp value";
            }
            return NUdf::TUnboxedValuePod(value.uint64_value());
        }
        case NUdf::TDataType<NUdf::TInterval>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Interval");
            if ((ui64)std::abs(value.int64_value()) >= NUdf::MAX_TIMESTAMP) {
                throw yexception() << "Invalid Interval value";
            }
            return NUdf::TUnboxedValuePod(value.int64_value());
        }
        case NUdf::TDataType<NUdf::TDate32>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kUint32Value, "Date32");
            if (value.int32_value() < NUdf::MIN_DATE32 || value.int32_value() > NUdf::MAX_DATE32) {
                throw yexception() << "Invalid Date value";
            }
            return NUdf::TUnboxedValuePod(value.int32_value());
        }
        case NUdf::TDataType<NUdf::TDatetime64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Datetime64");
            if (value.int64_value() < NUdf::MIN_DATETIME64 || value.int64_value() > NUdf::MAX_DATETIME64) {
                throw yexception() << "Invalid Datetime64 value";
            }
            return NUdf::TUnboxedValuePod(value.int64_value());
        }
        case NUdf::TDataType<NUdf::TTimestamp64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Timestamp64");
            if (value.int64_value() < NUdf::MIN_TIMESTAMP64 || value.int64_value() > NUdf::MAX_TIMESTAMP64) {
                throw yexception() << "Invalid Timestamp64 value";
            }
            return NUdf::TUnboxedValuePod(value.int64_value());
        }
        case NUdf::TDataType<NUdf::TInterval64>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kInt64Value, "Interval64");
            if (std::abs(value.int64_value()) > NUdf::MAX_INTERVAL64) {
                throw yexception() << "Invalid Interval64 value";
            }
            return NUdf::TUnboxedValuePod(value.int64_value());
        }
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kLow128, "Uuid");
            union {
                ui64 half[2];
                char bytes[16];
            } buf;
            buf.half[0] = value.low_128();
            buf.half[1] = value.high_128();
            return MakeString(NUdf::TStringRef(buf.bytes, 16));
        }
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "JsonDocument");
            const auto binaryJson = NBinaryJson::SerializeToBinaryJson(value.text_value());
            if (!binaryJson.Defined()) {
                throw yexception() << "Invalid JsonDocument value";
            }
            return MakeString(TStringBuf(binaryJson->Data(), binaryJson->Size()));
        }
        case NUdf::TDataType<NUdf::TDyNumber>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kTextValue, "DyNumber");
            const auto dyNumber = NDyNumber::ParseDyNumberString(value.text_value());
            if (!dyNumber.Defined()) {
                throw yexception() << "Invalid DyNumber value";
            }
            return MakeString(*dyNumber);
        }
        case NUdf::TDataType<char*>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kBytesValue, "String");
            return MakeString(value.bytes_value());
        }
        case NUdf::TDataType<NUdf::TYson>::Id: {
            CheckTypeId(value.value_case(), Ydb::Value::kBytesValue, "Yson");
            const auto& stringRef = value.bytes_value();
            if (!NYql::NDom::IsValidYson(stringRef)) {
                throw yexception() << "Invalid Yson value";
            }
            return MakeString(value.bytes_value());
        }
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            return NUdf::TUnboxedValuePod(NYql::NDecimal::FromHalfs(value.low_128(), value.high_128()));
        }
        default: {
            throw yexception() << "Unsupported data type: " << dataType->GetSchemeType();
        }
    }
}

NUdf::TUnboxedValue TProtoImporter::ImportValueFromProto(const TType* type, const Ydb::Value& value, const THolderFactory& factory) {
    switch (type->GetKind()) {
    case TType::EKind::Void:
        return NUdf::TUnboxedValuePod::Void();

    case TType::EKind::Null:
    case TType::EKind::EmptyList:
    case TType::EKind::EmptyDict:
        return NUdf::TUnboxedValue();

    case TType::EKind::Data:
        return KindDataImport(type, value);

    case TType::EKind::Optional: {
        const TType* innerType = type;
        const Ydb::Value* innerValue = &value;
        ui32 level = 0;
        ui32 nestLevel = 0;
        while(innerType->GetKind() == TType::EKind::Optional) {
            const TOptionalType* optionalType = static_cast<const TOptionalType*>(innerType);
            innerType = optionalType->GetItemType();
            ++level;
            if (innerValue->value_case() == Ydb::Value::kNestedValue) {
                innerValue = &(innerValue->nested_value());
                ++nestLevel;
            }
        }

        const Ydb::Value* tmpValue = innerValue;
        while(tmpValue->value_case() == Ydb::Value::kNestedValue) {
            tmpValue = &(tmpValue->nested_value());
        }

        if (innerType->GetKind() == TType::EKind::Variant) {
            if (tmpValue->value_case() == Ydb::Value::kNullFlagValue) {
                auto res = ImportValueFromProto(innerType, *innerValue, factory);
                while (level-->0) { res = res.MakeOptional(); }
                return res;
            }
            auto res = ImportValueFromProto(innerType, value, factory);
            while (level-->0) { res = res.MakeOptional(); }
            return res;
        } else {
            MKQL_ENSURE(innerValue->value_case() != Ydb::Value::kNestedValue, "unexpected nested value");
            if (innerValue->value_case() != Ydb::Value::kNullFlagValue) {
                auto res = ImportValueFromProto(innerType, *innerValue, factory);
                while (level-->0) { res = res.MakeOptional(); }
                return res;
            } else {
                auto res = NUdf::TUnboxedValue();
                while (nestLevel-->0) { res = res.MakeOptional(); }
                return res;
            }
        }
    }

    case TType::EKind::List: {
        const TListType* listType = static_cast<const TListType*>(type);
        auto itemType = listType->GetItemType();
        const auto& list = value.items();
        NUdf::TUnboxedValue *items = nullptr;
        auto array = factory.CreateDirectArrayHolder(list.size(), items);
        for (const auto& x : list) {
            *items++ = ImportValueFromProto(itemType, x, factory);
        }

        return array;
    }

    case TType::EKind::Struct: {
        const TStructType* structType = static_cast<const TStructType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = factory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);
        TRemapArray remap = TRemapArray::FromCookie(structType->GetCookie());
        MKQL_ENSURE((ui32)value.items_size() == structType->GetMembersCount(),
            "Member size mismatch. members in value: " << value.items_size()
            << ", members in type: " << structType->GetMembersCount());
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            ui32 remapped = remap.empty() ? index : remap[index];
            auto memberType = structType->GetMemberType(index);
            itemsPtr[index] = ImportValueFromProto(memberType, value.items(remapped), factory);
        }

        return res;
    }

    case TType::EKind::Tuple: {
        const TTupleType* tupleType = static_cast<const TTupleType*>(type);
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto res = factory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);
        MKQL_ENSURE_S((ui32)value.items_size() == tupleType->GetElementsCount(),
            "Elements size mismatch. Elements in value: " << value.items_size()
            << ", elements in type: " << tupleType->GetElementsCount());
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = tupleType->GetElementType(index);
            itemsPtr[index] = ImportValueFromProto(elementType, value.items(index), factory);
        }

        return res;
    }

    case TType::EKind::Dict: {
        const TDictType* dictType = static_cast<const TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        auto dictBuilder = factory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

        for (const auto& x : value.pairs()) {
            dictBuilder->Add(
                ImportValueFromProto(keyType, x.key(), factory),
                ImportValueFromProto(payloadType, x.payload(), factory)
            );
        }

        return dictBuilder->Build();
    }

    case TType::EKind::Variant: {
        const TVariantType* variantType = static_cast<const TVariantType*>(type);
        auto index = value.variant_index();
        MKQL_ENSURE_S(index < variantType->GetAlternativesCount(), "type has " << variantType->GetAlternativesCount()
            << " alternatives, but requested " << index);
        auto alternative = variantType->GetAlternativeType(index);
        if (value.value_case() == Ydb::Value::kNestedValue) {
            auto unboxedValue = ImportValueFromProto(alternative, value.nested_value(), factory);
            return factory.CreateVariantHolder(std::move(unboxedValue.Release()), index);
        }
        auto unboxedValue = ImportValueFromProto(alternative, value, factory);
        return factory.CreateVariantHolder(std::move(unboxedValue.Release()), index);
    }

    case TType::EKind::Tagged: {
        const TTaggedType* taggedType = static_cast<const TTaggedType*>(type);
        auto unboxedValue = ImportValueFromProto(taggedType->GetBaseType(), value, factory);
        return unboxedValue;
    }

    case TType::EKind::Pg: {
        if (value.GetValueCase() == Ydb::Value::kNullFlagValue) {
            return NYql::NUdf::TUnboxedValue();
        }
        const TPgType* pgType = static_cast<const TPgType*>(type);
        NYql::NUdf::TUnboxedValue unboxedValue;
        if (value.Hastext_value()) {
            unboxedValue = NYql::NCommon::PgValueFromNativeText(value.Gettext_value(), pgType->GetTypeId());
        } else if (value.Hasbytes_value()) {
            unboxedValue = NYql::NCommon::PgValueFromNativeBinary(value.Getbytes_value(), pgType->GetTypeId());
        } else {
            MKQL_ENSURE(false, "malformed pg value");
        }
        return unboxedValue;
    }

    default:
        MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
    }
}

NUdf::TUnboxedValue TProtoImporter::ImportValueFromProto(const TType* type, const NKikimrMiniKQL::TValue& value, const THolderFactory& factory) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
            return NUdf::TUnboxedValuePod::Void();

        case TType::EKind::Null:
            return NUdf::TUnboxedValuePod();

        case TType::EKind::Data:
            return HandleKindDataImport(type, value);

        case TType::EKind::Pg: {
            auto pgType = static_cast<const TPgType*>(type);
            if (value.GetValueValueCase() == NKikimrMiniKQL::TValue::kNullFlagValue) {
                return NUdf::TUnboxedValue();
            }
            if (value.HasBytes()) {
                return NYql::NCommon::PgValueFromNativeBinary(value.GetBytes(), pgType->GetTypeId());
            }
            if (value.HasText()) {
                return NYql::NCommon::PgValueFromNativeText(value.GetBytes(), pgType->GetTypeId());
            }
            MKQL_ENSURE(false, "malformed pg value");
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            if (value.HasOptional()) {
                const TType* itemType = optionalType->GetItemType();
                return ImportValueFromProto(itemType, value.GetOptional(), factory).MakeOptional();
            }
            else {
                return NUdf::TUnboxedValue();
            }
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            const TType* itemType = listType->GetItemType();
            const auto& list = value.GetList();
            NUdf::TUnboxedValue *items = nullptr;
            auto array = factory.CreateDirectArrayHolder(list.size(), items);
            for (const auto& x : list) {
                *items++ = ImportValueFromProto(itemType, x, factory);
            }

            return std::move(array);
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto res = factory.CreateDirectArrayHolder(structType->GetMembersCount(), itemsPtr);
            TRemapArray remap = TRemapArray::FromCookie(structType->GetCookie());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                ui32 remapped = remap.empty() ? index : remap[index];
                const TType* memberType = structType->GetMemberType(index);
                itemsPtr[index] = ImportValueFromProto(memberType, value.GetStruct(remapped), factory);
            }

            return std::move(res);
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto res = factory.CreateDirectArrayHolder(tupleType->GetElementsCount(), itemsPtr);
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                const TType* elementType = tupleType->GetElementType(index);
                itemsPtr[index] = ImportValueFromProto(elementType, value.GetTuple(index), factory);
            }

            return std::move(res);
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            const TType* keyType = dictType->GetKeyType();
            const TType* payloadType = dictType->GetPayloadType();
            auto dictBuilder = factory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);

            for (const auto& x : value.GetDict()) {
                dictBuilder->Add(
                        ImportValueFromProto(keyType, x.GetKey(), factory),
                        ImportValueFromProto(payloadType, x.GetPayload(), factory)
                );
            }

            return dictBuilder->Build();
        }

        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unknown kind: " << type->GetKindAsStr());
    }
}

std::pair<TType*, NUdf::TUnboxedValue> ImportValueFromProto(const NKikimrMiniKQL::TType& type,
    const NKikimrMiniKQL::TValue& value, const TTypeEnvironment& env, const THolderFactory& factory)
{
    TProtoImporter importer(env);
    TType* nodeType = importer.ImportTypeFromProto(type);
    auto unboxedValue = importer.ImportValueFromProto(nodeType, value, factory);
    return {nodeType, unboxedValue};
}

std::pair<TType*, NUdf::TUnboxedValue> ImportValueFromProto(const Ydb::Type& type,
    const Ydb::Value& value, const TTypeEnvironment& env, const THolderFactory& factory)
{
    TProtoImporter importer(env);
    TType* nodeType = importer.ImportTypeFromProto(type);
    auto unboxedValue = importer.ImportValueFromProto(nodeType, value, factory);
    return {nodeType, unboxedValue};
}

NUdf::TUnboxedValue ImportValueFromProto(TType* type,
    const Ydb::Value& value, const TTypeEnvironment& env, const THolderFactory& factory)
{
    TProtoImporter importer(env);
    auto unboxedValue = importer.ImportValueFromProto(type, value, factory);
    return unboxedValue;

}

TType* ImportTypeFromProto(const NKikimrMiniKQL::TType& type, const TTypeEnvironment& env) {
    TProtoImporter importer(env);
    return importer.ImportTypeFromProto(type);
}

TRuntimeNode ImportValueFromProto(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
                                  const TTypeEnvironment& env)
{
    TProtoImporter importer(env);
    TType* nodeType = importer.ImportTypeFromProto(type);
    TNode* nodeValue = importer.ImportNodeFromProto(nodeType, value);
    return TRuntimeNode(nodeValue, true);
}

TRuntimeNode ImportValueFromProto(const NKikimrMiniKQL::TParams& params, const TTypeEnvironment& env) {
    if (params.HasType() && params.HasValue()) {
        return ImportValueFromProto(params.GetType(), params.GetValue(), env);
    }
    return TRuntimeNode(env.GetEmptyStructLazy(), true);
}

}
