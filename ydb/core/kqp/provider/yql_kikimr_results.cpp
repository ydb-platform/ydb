#include "yql_kikimr_results.h"

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/result_format/yql_codec_results.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ResultsOverflow(ui64 rows, ui64 bytes, const IDataProvider::TFillSettings& fillSettings) {
    if (fillSettings.RowsLimitPerWrite && rows >= *fillSettings.RowsLimitPerWrite) {
        return true;
    }

    if (fillSettings.AllResultsBytesLimit && bytes >= *fillSettings.AllResultsBytesLimit) {
        return true;
    }

    return false;
}

void WriteValueToYson(const TStringStream& stream, NResult::TYsonResultWriter& writer, const NKikimrMiniKQL::TType& type,
    const NKikimrMiniKQL::TValue& value, const TColumnOrder* fieldsOrder,
    const IDataProvider::TFillSettings& fillSettings, bool& truncated, bool firstLevel = false)
{
    switch (type.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void:
            writer.OnVoid();
            return;

        case NKikimrMiniKQL::ETypeKind::Data:
        {
            if (type.GetData().GetScheme() == NYql::NProto::TypeIds::Decimal) {
                using NYql::NDecimal::ToString;
                using NYql::NDecimal::FromProto;

                auto decimalParams = type.GetData().GetDecimalParams();
                const auto& str = ToString(FromProto(value), decimalParams.GetPrecision(), decimalParams.GetScale());
                writer.OnUtf8StringScalar(str);

                return;
            }

            if (type.GetData().GetScheme() == NYql::NProto::TypeIds::Uuid) {
                using NKikimr::NUuid::UuidHalfsToByteString;

                TStringStream stream;
                UuidHalfsToByteString(value.GetLow128(), value.GetHi128(), stream);
                writer.OnStringScalar(stream.Str());

                return;
            }

            if (type.GetData().GetScheme() == NYql::NProto::TypeIds::DyNumber) {
                using NKikimr::NDyNumber::DyNumberToString;

                const auto number = DyNumberToString(value.GetBytes());
                YQL_ENSURE(number.Defined(), "Invalid DyNumber binary representation");
                writer.OnStringScalar(*number);

                return;
            }

            if (type.GetData().GetScheme() == NYql::NProto::TypeIds::JsonDocument) {
                using NKikimr::NBinaryJson::SerializeToJson;

                const auto json = SerializeToJson(value.GetBytes());
                writer.OnStringScalar(json);

                return;
            }

            if (type.GetData().GetScheme() == NYql::NProto::TypeIds::Yson) {
                writer.OnRaw(value.GetBytes(), NYT::NYson::EYsonType::Node);
                return;
            }

            if (value.HasBool()) {
                writer.OnBooleanScalar(value.GetBool());
            }

            if (value.HasInt32()) {
                writer.OnInt64Scalar(value.GetInt32());
            }

            if (value.HasUint32()) {
                writer.OnUint64Scalar(value.GetUint32());
            }

            if (value.HasInt64()) {
                writer.OnInt64Scalar(value.GetInt64());
            }

            if (value.HasUint64()) {
                writer.OnUint64Scalar(value.GetUint64());
            }

            if (value.HasFloat()) {
                writer.OnFloatScalar(value.GetFloat());
            }

            if (value.HasDouble()) {
                writer.OnDoubleScalar(value.GetDouble());
            }

            if (value.HasBytes()) {
                writer.OnStringScalar(value.GetBytes());
            }

            if (value.HasText()) {
                writer.OnStringScalar(value.GetText());
            }

            return;
        }

        case NKikimrMiniKQL::ETypeKind::Pg:
        {
            if (value.GetValueValueCase() == NKikimrMiniKQL::TValue::kNullFlagValue) {
                writer.OnEntity();
            } else if (value.HasBytes()) {
                auto convert = NKikimr::NPg::PgNativeTextFromNativeBinary(
                    value.GetBytes(), NKikimr::NPg::TypeDescFromPgTypeId(type.GetPg().Getoid())
                );
                YQL_ENSURE(!convert.Error, "Failed to convert pg value to text: " << *convert.Error);
                writer.OnStringScalar(convert.Str);
            } else if (value.HasText()) {
                writer.OnStringScalar(value.GetText());
            } else {
                YQL_ENSURE(false, "malformed pg value");
            }
            return;
        }

        case NKikimrMiniKQL::ETypeKind::Optional:
            if (!value.HasOptional()) {
                writer.OnEntity();
                return;
            }

            writer.OnBeginList();
            writer.OnListItem();
            WriteValueToYson(stream, writer, type.GetOptional().GetItem(), value.GetOptional(),
                nullptr, fillSettings, truncated);
            writer.OnEndList();
            return;

        case NKikimrMiniKQL::ETypeKind::Tuple: {
            writer.OnBeginList();
            auto tupleType = type.GetTuple();

            for (size_t i = 0; i < tupleType.ElementSize(); ++i) {
                auto element = value.GetTuple(i);
                auto elementType = tupleType.GetElement(i);

                writer.OnListItem();
                WriteValueToYson(stream, writer, elementType, element, nullptr, fillSettings, truncated);
            }

            writer.OnEndList();
            return;
        }

        case NKikimrMiniKQL::ETypeKind::List: {
            writer.OnBeginList();
            ui64 rowsWritten = 0;
            for (auto& item : value.GetList()) {
                writer.OnListItem();

                if (firstLevel) {
                    if (ResultsOverflow(rowsWritten, stream.Size(), fillSettings)) {
                        truncated = true;
                        break;
                    }
                }

                WriteValueToYson(stream, writer, type.GetList().GetItem(), item,
                    firstLevel ? fieldsOrder : nullptr, fillSettings, truncated);
                ++rowsWritten;
            }
            writer.OnEndList();
            return;
        }

        case NKikimrMiniKQL::ETypeKind::Struct:
        {
            writer.OnBeginList();
            auto structType = type.GetStruct();

            auto writeMember = [&stream, &structType, &value, &writer, &fillSettings, &truncated] (size_t index) {
                auto member = structType.GetMember(index);
                auto memberValue = value.GetStruct(index);
                writer.OnListItem();
                WriteValueToYson(stream, writer, member.GetType(), memberValue, nullptr,
                    fillSettings, truncated);
            };

            if (fieldsOrder) {
                YQL_ENSURE(fieldsOrder->Size() == structType.MemberSize());
                TMap<TString, size_t> memberIndices;
                for (size_t i = 0; i < structType.MemberSize(); ++i) {
                    memberIndices[structType.GetMember(i).GetName()] = i;
                }
                for (auto& field : *fieldsOrder) {
                    auto* memberIndex = memberIndices.FindPtr(field.PhysicalName);
                    YQL_ENSURE(memberIndex);

                    writeMember(*memberIndex);
                }
            } else {
                for (size_t i = 0; i < structType.MemberSize(); ++i) {
                    writeMember(i);
                }
            }

            writer.OnEndList();
            return;
        }

        case NKikimrMiniKQL::ETypeKind::Dict:
        {
            writer.OnBeginList();
            auto dictType = type.GetDict();
            auto keyType = dictType.GetKey();
            auto payloadType = dictType.GetPayload();

            for (auto& pair : value.GetDict()) {
                writer.OnListItem();

                writer.OnBeginList();
                writer.OnListItem();
                WriteValueToYson(stream, writer, keyType, pair.GetKey(), nullptr, fillSettings, truncated);
                writer.OnListItem();
                WriteValueToYson(stream, writer, payloadType, pair.GetPayload(), nullptr, fillSettings, truncated);
                writer.OnEndList();
            }

            writer.OnEndList();
            return;
        }

        default:
            YQL_ENSURE(false, "Unsupported type: " + ToString((ui32)type.GetKind()));
    }
}


} // namespace

void KikimrResultToYson(const TStringStream& stream, NYson::TYsonWriter& writer, const NKikimrMiniKQL::TResult& result,
    const TColumnOrder& columnHints, const IDataProvider::TFillSettings& fillSettings, bool& truncated)
{
    truncated = false;
    NResult::TYsonResultWriter resultWriter(writer);
    WriteValueToYson(stream, resultWriter, result.GetType(), result.GetValue(), columnHints.Size() == 0 ? nullptr : &columnHints,
        fillSettings, truncated, true);
}


const TTypeAnnotationNode* ParseTypeFromYdbType(const Ydb::Type& type, TExprContext& ctx) {
    switch (type.type_case()) {
        case Ydb::Type::kVoidType: {
            return ctx.MakeType<TVoidExprType>();
        }

        case Ydb::Type::kTypeId: {
            auto slot = NKikimr::NUdf::FindDataSlot(type.type_id());
            if (!slot) {
                ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported data type: "
                    << type.ShortDebugString()));

                return nullptr;
            }
            return ctx.MakeType<TDataExprType>(*slot);
        }

        case Ydb::Type::kDecimalType: {
            auto slot = NKikimr::NUdf::FindDataSlot(NYql::NProto::TypeIds::Decimal);
            if (!slot) {
                ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported data type: "
                    << type.ShortDebugString()));

                return nullptr;
            }

            return ctx.MakeType<TDataExprParamsType>(*slot, ToString(type.decimal_type().precision()), ToString(type.decimal_type().scale()));
        }

        case Ydb::Type::kOptionalType: {
            auto itemType = ParseTypeFromYdbType(type.optional_type().item(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TOptionalExprType>(itemType);
        }

        case Ydb::Type::kTupleType: {
            TTypeAnnotationNode::TListType tupleItems;

            for (auto& element : type.tuple_type().Getelements()) {
                auto elementType = ParseTypeFromYdbType(element, ctx);
                if (!elementType) {
                    return nullptr;
                }

                tupleItems.push_back(elementType);
            }

            return ctx.MakeType<TTupleExprType>(tupleItems);
        }

        case Ydb::Type::kListType: {
            auto itemType = ParseTypeFromYdbType(type.list_type().item(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TListExprType>(itemType);
        }

        case Ydb::Type::kStructType: {
            TVector<const TItemExprType*> structMembers;
            for (auto& member : type.struct_type().Getmembers()) {
                auto memberType = ParseTypeFromYdbType(member.Gettype(), ctx);
                if (!memberType) {
                    return nullptr;
                }

                structMembers.push_back(ctx.MakeType<TItemExprType>(member.Getname(), memberType));
            }

            return ctx.MakeType<TStructExprType>(structMembers);
        }

        case Ydb::Type::kDictType: {
            auto keyType = ParseTypeFromYdbType(type.dict_type().key(), ctx);
            if (!keyType) {
                return nullptr;
            }

            auto payloadType = ParseTypeFromYdbType(type.dict_type().payload(), ctx);
            if (!payloadType) {
                return nullptr;
            }

            return ctx.MakeType<TDictExprType>(keyType, payloadType);
        }

        case Ydb::Type::kPgType: {
            if (!type.pg_type().type_name().empty()) {
                const auto& typeName = type.pg_type().type_name();
                auto typeDesc = NKikimr::NPg::TypeDescFromPgTypeName(typeName);
                return ctx.MakeType<TPgExprType>(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
            }
            return ctx.MakeType<TPgExprType>(type.pg_type().Getoid());
        }

        case Ydb::Type::kNullType:
            [[fallthrough]];
        default: {
            ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported protobuf type: "
                << type.ShortDebugString()));
            return nullptr;
        }
    }
}

} // namespace NYql
