#include "yql_kikimr_results.h"

#include <ydb/library/binary_json/read.h>
#include <ydb/library/dynumber/dynumber.h>

#include <ydb/library/yql/providers/common/codec/yql_codec_results.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

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

void WriteValueToYson(const TStringStream& stream, NCommon::TYsonResultWriter& writer, const NKikimrMiniKQL::TType& type,
    const NKikimrMiniKQL::TValue& value, const TVector<TString>* fieldsOrder,
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
                using NKikimr::NMiniKQL::UuidHalfsToByteString;

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
                YQL_ENSURE(fieldsOrder->size() == structType.MemberSize());
                TMap<TString, size_t> memberIndices;
                for (size_t i = 0; i < structType.MemberSize(); ++i) {
                    memberIndices[structType.GetMember(i).GetName()] = i;
                }
                for (auto& field : *fieldsOrder) {
                    auto* memberIndex = memberIndices.FindPtr(field);
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

TExprNode::TPtr MakeAtomForDataType(EDataSlot slot, const NKikimrMiniKQL::TValue& value,
    TPositionHandle pos, TExprContext& ctx)
{
    if (slot == EDataSlot::Bool) {
        return ctx.NewAtom(pos, ToString(value.GetBool()));
    } else if (slot == EDataSlot::Uint8) {
        return ctx.NewAtom(pos, ToString(ui8(value.GetUint32())));
    } else if (slot == EDataSlot::Int8) {
        return ctx.NewAtom(pos, ToString(i8(value.GetInt32())));
    } else if (slot == EDataSlot::Int16) {
        return ctx.NewAtom(pos, ToString(i16(value.GetInt32())));
    } else if (slot == EDataSlot::Uint16) {
        return ctx.NewAtom(pos, ToString(ui16(value.GetUint32())));
    } else if (slot == EDataSlot::Int32) {
        return ctx.NewAtom(pos, ToString(value.GetInt32()));
    } else if (slot == EDataSlot::Uint32) {
        return ctx.NewAtom(pos, ToString(value.GetUint32()));
    } else if (slot == EDataSlot::Int64) {
        return ctx.NewAtom(pos, ToString(value.GetInt64()));
    } else if (slot == EDataSlot::Uint64) {
        return ctx.NewAtom(pos, ToString(value.GetUint64()));
    } else if (slot == EDataSlot::Float) {
        return ctx.NewAtom(pos, ToString(value.GetFloat()));
    } else if (slot == EDataSlot::Double) {
        return ctx.NewAtom(pos, ToString(value.GetDouble()));
    } else if (slot == EDataSlot::String) {
        return ctx.NewAtom(pos, value.GetBytes());
    } else if (slot == EDataSlot::Utf8) {
        return ctx.NewAtom(pos, value.GetText());
    } else if (slot == EDataSlot::Yson) {
        return ctx.NewAtom(pos, value.GetBytes());
    } else if (slot == EDataSlot::Json) {
        return ctx.NewAtom(pos, value.GetText());
    } else if (slot == EDataSlot::Date) {
        return ctx.NewAtom(pos, ToString(ui16(value.GetUint32())));
    } else if (slot == EDataSlot::Datetime) {
        return ctx.NewAtom(pos, ToString(value.GetUint32()));
    } else if (slot == EDataSlot::Timestamp) {
        return ctx.NewAtom(pos, ToString(value.GetUint64()));
    } else if (slot == EDataSlot::Interval) {
        return ctx.NewAtom(pos, ToString(value.GetInt64()));
    } else {
       return nullptr;
    }
}

bool IsSameType(const NKikimrMiniKQL::TDataType& actual, const NKikimrMiniKQL::TDataType& expected) {
        return actual.GetScheme() == expected.GetScheme() &&
            actual.GetDecimalParams().GetPrecision() == expected.GetDecimalParams().GetPrecision() &&
            actual.GetDecimalParams().GetScale() == expected.GetDecimalParams().GetScale();
}

bool IsSameType(const NKikimrMiniKQL::TTupleType& actual, const NKikimrMiniKQL::TTupleType& expected) {
    size_t size = actual.ElementSize();
    if (size != expected.ElementSize()) {
        return false;
    }
    for (size_t i = 0; i < size; ++i) {
        if (!NYql::IsSameType(actual.GetElement(i), expected.GetElement(i))) {
            return false;
        }
    }
    return true;
}

bool IsSameType(const NKikimrMiniKQL::TStructType& actual, const NKikimrMiniKQL::TStructType& expected) {
    size_t size = actual.MemberSize();
    if (size != expected.MemberSize()) {
        return false;
    }
    std::map<TString, NKikimrMiniKQL::TType> expected_fields;
    for (size_t i = 0; i < size; ++i) {
        auto& st = expected.GetMember(i);
        expected_fields.emplace(st.GetName(), st.GetType());
    }
    for (size_t i = 0; i < size; ++i) {
        auto& f = actual.GetMember(i);
        auto it = expected_fields.find(f.GetName());
        if (it == expected_fields.end()) {
            return false;
        }

        if (!NYql::IsSameType(f.GetType(), it->second)) {
            return false;
        }
    }
    return true;
}

bool IsSameType(const NKikimrMiniKQL::TVariantType& actual, const NKikimrMiniKQL::TVariantType& expected) {
    if (actual.GetTypeCase() != expected.GetTypeCase()) {
        return false;
    }
    switch (actual.GetTypeCase()) {
        case NKikimrMiniKQL::TVariantType::kTupleItems:
            return IsSameType(actual.GetTupleItems(), expected.GetTupleItems());
        case NKikimrMiniKQL::TVariantType::kStructItems:
            return IsSameType(actual.GetStructItems(), expected.GetStructItems());
        case NKikimrMiniKQL::TVariantType::TYPE_NOT_SET:
            Y_ENSURE(false, "Variant type not set");
            return false;
    }
}

} // namespace

bool GetRunResultIndex(const NKikimrMiniKQL::TStructType& resultType, const TString& resultName, ui32& index) {
    for (ui32 i = 0; i < resultType.MemberSize(); ++i) {
        if (resultType.GetMember(i).GetName() == resultName) {
            index = i;
            return true;
        }
    }

    return false;
}

void UnpackKikimrRunResult(const NKikimrMiniKQL::TResult& runResult, const NKikimrMiniKQL::TTupleType*& tupleType,
    const NKikimrMiniKQL::TValue*& tupleValue)
{
    const auto& structType = runResult.GetType().GetStruct();
    ui32 resultIndex;
    YQL_ENSURE(GetRunResultIndex(structType, "Result", resultIndex));

    const auto& member = structType.GetMember(resultIndex);
    const auto& memberType = member.GetType();
    YQL_ENSURE(memberType.GetKind() == NKikimrMiniKQL::ETypeKind::Optional);
    const auto& optionalType = memberType.GetOptional().GetItem();
    YQL_ENSURE(optionalType.GetKind() == NKikimrMiniKQL::ETypeKind::Tuple);
    tupleType = &optionalType.GetTuple();

    const auto& structValue = runResult.GetValue().GetStruct(resultIndex);
    YQL_ENSURE(structValue.HasOptional());
    tupleValue = &structValue.GetOptional();
}

void GetKikimrUnpackedRunResult(const NKikimrMiniKQL::TResult& runResult, ui32 index,
    const NKikimrMiniKQL::TType*& type, const NKikimrMiniKQL::TValue*& value)
{
    const NKikimrMiniKQL::TTupleType* tupleType;
    const NKikimrMiniKQL::TValue* tupleValue;
    UnpackKikimrRunResult(runResult, tupleType, tupleValue);

    type = &tupleType->GetElement(index);
    value = &tupleValue->GetTuple(index);
}

NKikimrMiniKQL::TResult GetKikimrUnpackedRunResult(const NKikimrMiniKQL::TResult& runResult, ui32 index) {
    const NKikimrMiniKQL::TType* type;
    const NKikimrMiniKQL::TValue* value;
    GetKikimrUnpackedRunResult(runResult, index, type, value);
    YQL_ENSURE(type);
    YQL_ENSURE(value);

    NKikimrMiniKQL::TResult result;
    result.MutableType()->CopyFrom(*type);
    result.MutableValue()->CopyFrom(*value);
    return result;
}

TVector<NKikimrMiniKQL::TResult*> UnpackKikimrRunResult(const NKikimrMiniKQL::TResult& runResult, google::protobuf::Arena* arena) {
    const NKikimrMiniKQL::TTupleType* tupleType;
    const NKikimrMiniKQL::TValue* tupleValue;
    UnpackKikimrRunResult(runResult, tupleType, tupleValue);

    TVector<NKikimrMiniKQL::TResult*> results;
    ui32 resultsSize = tupleType->ElementSize();
    for (ui32 i = 0; i < resultsSize; ++i) {
        results.push_back(google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(arena));
        NKikimrMiniKQL::TResult* result = results.back();
        *result->MutableType() = tupleType->GetElement(i);
        *result->MutableValue() = tupleValue->GetTuple(i);
    }

    return results;
}

void KikimrResultToYson(const TStringStream& stream, NYson::TYsonWriter& writer, const NKikimrMiniKQL::TResult& result,
    const TVector<TString>& columnHints, const IDataProvider::TFillSettings& fillSettings, bool& truncated)
{
    truncated = false;
    NCommon::TYsonResultWriter resultWriter(writer);
    WriteValueToYson(stream, resultWriter, result.GetType(), result.GetValue(), columnHints.empty() ? nullptr : &columnHints,
        fillSettings, truncated, true);
}

bool IsRawKikimrResult(const NKikimrMiniKQL::TResult& result) {
    auto& type = result.GetType();
    if (type.GetKind() != NKikimrMiniKQL::ETypeKind::Struct) {
        return true;
    }

    auto& structType = type.GetStruct();
    if (structType.MemberSize() != 2) {
        return true;
    }

    return structType.GetMember(0).GetName() != "Data" || structType.GetMember(1).GetName() != "Truncated";
}

NKikimrMiniKQL::TResult* KikimrResultToProto(const NKikimrMiniKQL::TResult& result, const TVector<TString>& columnHints,
    const IDataProvider::TFillSettings& fillSettings, google::protobuf::Arena* arena)
{
    NKikimrMiniKQL::TResult* packedResult = google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(arena);
    auto* packedType = packedResult->MutableType();
    packedType->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto* dataMember = packedType->MutableStruct()->AddMember();
    dataMember->SetName("Data");
    auto* truncatedMember = packedType->MutableStruct()->AddMember();
    truncatedMember->SetName("Truncated");
    truncatedMember->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
    truncatedMember->MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<bool>::Id);

    auto* packedValue = packedResult->MutableValue();
    auto* dataValue = packedValue->AddStruct();
    auto* dataType = dataMember->MutableType();
    auto* truncatedValue = packedValue->AddStruct();

    bool truncated = false;
    if (result.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::List) {
        const auto& itemType = result.GetType().GetList().GetItem();

        TMap<TString, size_t> memberIndices;
        if (itemType.GetKind() == NKikimrMiniKQL::ETypeKind::Struct && !columnHints.empty()) {
            const auto& structType = itemType.GetStruct();

            for (size_t i = 0; i < structType.MemberSize(); ++i) {
                memberIndices[structType.GetMember(i).GetName()] = i;
            }

            dataType->SetKind(NKikimrMiniKQL::ETypeKind::List);
            auto* newItem = dataType->MutableList()->MutableItem();
            newItem->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
            auto* newStructType = newItem->MutableStruct();
            for (auto& column : columnHints) {
                auto* memberIndex = memberIndices.FindPtr(column);
                YQL_ENSURE(memberIndex);

                *newStructType->AddMember() = structType.GetMember(*memberIndex);
            }
        } else {
            *dataType = result.GetType();
        }

        ui64 rowsWritten = 0;
        ui64 bytesWritten = 0;
        for (auto& item : result.GetValue().GetList()) {
            if (ResultsOverflow(rowsWritten, bytesWritten, fillSettings)) {
                truncated = true;
                break;
            }

            if (!memberIndices.empty()) {
                auto* newStruct = dataValue->AddList();
                for (auto& column : columnHints) {
                    auto* memberIndex = memberIndices.FindPtr(column);
                    YQL_ENSURE(memberIndex);

                    *newStruct->AddStruct() = item.GetStruct(*memberIndex);
                }
            } else {
                *dataValue->AddList() = item;
            }

            bytesWritten += item.ByteSize();
            ++rowsWritten;
        }
    } else {
        dataType->CopyFrom(result.GetType());
        dataValue->CopyFrom(result.GetValue());
    }

    truncatedValue->SetBool(truncated);
    return packedResult;
}

const TTypeAnnotationNode* ParseTypeFromKikimrProto(const NKikimrMiniKQL::TType& type, TExprContext& ctx) {
    switch (type.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void: {
            return ctx.MakeType<TVoidExprType>();
        }

        case NKikimrMiniKQL::ETypeKind::Data: {
            const NKikimrMiniKQL::TDataType& protoData = type.GetData();
            auto schemeType = protoData.GetScheme();
            auto slot = NKikimr::NUdf::FindDataSlot(schemeType);
            if (!slot) {
                ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported data type: "
                    << protoData.GetScheme()));
                return nullptr;
            }

            if (schemeType == NYql::NProto::TypeIds::Decimal) {
                return ctx.MakeType<TDataExprParamsType>(*slot, ToString(protoData.GetDecimalParams().GetPrecision()),
                    ToString(protoData.GetDecimalParams().GetScale()));
            } else {
                return ctx.MakeType<TDataExprType>(*slot);
            }
        }

        case NKikimrMiniKQL::ETypeKind::Optional: {
            auto itemType = ParseTypeFromKikimrProto(type.GetOptional().GetItem(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TOptionalExprType>(itemType);
        }

        case NKikimrMiniKQL::ETypeKind::Tuple: {
            TTypeAnnotationNode::TListType tupleItems;

            for (auto& element : type.GetTuple().GetElement()) {
                auto elementType = ParseTypeFromKikimrProto(element, ctx);
                if (!elementType) {
                    return nullptr;
                }

                tupleItems.push_back(elementType);
            }

            return ctx.MakeType<TTupleExprType>(tupleItems);
        }

        case NKikimrMiniKQL::ETypeKind::List: {
            auto itemType = ParseTypeFromKikimrProto(type.GetList().GetItem(), ctx);
            if (!itemType) {
                return nullptr;
            }

            return ctx.MakeType<TListExprType>(itemType);
        }

        case NKikimrMiniKQL::ETypeKind::Struct: {
            TVector<const TItemExprType*> structMembers;
            for (auto& member : type.GetStruct().GetMember()) {
                auto memberType = ParseTypeFromKikimrProto(member.GetType(), ctx);
                if (!memberType) {
                    return nullptr;
                }

                structMembers.push_back(ctx.MakeType<TItemExprType>(member.GetName(), memberType));
            }

            return ctx.MakeType<TStructExprType>(structMembers);
        }

        case NKikimrMiniKQL::ETypeKind::Dict: {
            auto keyType = ParseTypeFromKikimrProto(type.GetDict().GetKey(), ctx);
            if (!keyType) {
                return nullptr;
            }

            auto payloadType = ParseTypeFromKikimrProto(type.GetDict().GetPayload(), ctx);
            if (!payloadType) {
                return nullptr;
            }

            return ctx.MakeType<TDictExprType>(keyType, payloadType);
        }

        default: {
            ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported protobuf type: "
                << type.ShortDebugString()));
            return nullptr;
        }
    }
}

bool ExportTypeToKikimrProto(const TTypeAnnotationNode& type, NKikimrMiniKQL::TType& protoType, TExprContext& ctx) {
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Void: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Void);
            return true;
        }

        case ETypeAnnotationKind::Data: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
            auto slot = type.Cast<TDataExprType>()->GetSlot();
            auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;
            if (typeId == NYql::NProto::TypeIds::Decimal) {
                auto dataProto = protoType.MutableData();
                dataProto->SetScheme(typeId);
                const auto paramsDataType = type.Cast<TDataExprParamsType>();
                ui8 precision = ::FromString<ui8>(paramsDataType->GetParamOne());
                ui8 scale = ::FromString<ui8>(paramsDataType->GetParamTwo());
                dataProto->MutableDecimalParams()->SetPrecision(precision);
                dataProto->MutableDecimalParams()->SetScale(scale);
            } else {
                protoType.MutableData()->SetScheme(typeId);
            }
            return true;
        }

        case ETypeAnnotationKind::Optional: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
            auto itemType = type.Cast<TOptionalExprType>()->GetItemType();
            return ExportTypeToKikimrProto(*itemType, *protoType.MutableOptional()->MutableItem(), ctx);
        }

        case ETypeAnnotationKind::Tuple: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
            auto& protoTuple = *protoType.MutableTuple();
            for (auto& itemType : type.Cast<TTupleExprType>()->GetItems()) {
                if (!ExportTypeToKikimrProto(*itemType, *protoTuple.AddElement(), ctx)) {
                    return false;
                }
            }
            return true;
        }

        case ETypeAnnotationKind::List: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::List);
            auto itemType = type.Cast<TListExprType>()->GetItemType();
            return ExportTypeToKikimrProto(*itemType, *protoType.MutableList()->MutableItem(), ctx);
        }

        case ETypeAnnotationKind::Struct: {
            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
            auto& protoStruct = *protoType.MutableStruct();
            for (auto& member : type.Cast<TStructExprType>()->GetItems()) {
                auto& protoMember = *protoStruct.AddMember();
                protoMember.SetName(TString(member->GetName()));
                if (!ExportTypeToKikimrProto(*member->GetItemType(), *protoMember.MutableType(), ctx)) {
                    return false;
                }
            }

            return true;
        }

        case ETypeAnnotationKind::Dict: {
            auto& dictType = *type.Cast<TDictExprType>();

            protoType.SetKind(NKikimrMiniKQL::ETypeKind::Dict);
            auto& protoDict = *protoType.MutableDict();

            if (!ExportTypeToKikimrProto(*dictType.GetKeyType(), *protoDict.MutableKey(), ctx)) {
                return false;
            }
            if (!ExportTypeToKikimrProto(*dictType.GetPayloadType(), *protoDict.MutablePayload(), ctx)) {
                return false;
            }

            return true;
        }

        default: {
            ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Unsupported protobuf type: " << type));
            return false;
        }
    }
}

TExprNode::TPtr ParseKikimrProtoValue(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
    TPositionHandle pos, TExprContext& ctx)
{
    auto position = ctx.GetPosition(pos);
    switch (type.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void: {
            return ctx.NewCallable(pos, "Void", {});
        }

        case NKikimrMiniKQL::ETypeKind::Data: {
            auto typeNode = ParseTypeFromKikimrProto(type, ctx);
            if (!typeNode) {
                return nullptr;
            }

            auto dataTypeNode = typeNode->Cast<TDataExprType>();
            YQL_ENSURE(dataTypeNode);

            auto valueAtom = MakeAtomForDataType(dataTypeNode->GetSlot(), value, pos, ctx);
            if (!valueAtom) {
                ctx.AddError(TIssue(position, TStringBuilder() << "Unsupported data type: "
                    << dataTypeNode->GetName()));
                return nullptr;
            }
            return ctx.NewCallable(pos, dataTypeNode->GetName(), {valueAtom});
        }

        case NKikimrMiniKQL::ETypeKind::Optional: {
            const auto& itemType = type.GetOptional().GetItem();

            if (value.HasOptional()) {
                auto itemNode = ParseKikimrProtoValue(itemType, value.GetOptional(), pos, ctx);
                if (!itemNode) {
                    return nullptr;
                }
                return ctx.NewCallable(pos, "Just", {itemNode});
            } else {
                auto typeNode = ParseTypeFromKikimrProto(type, ctx);
                if (!typeNode) {
                    return nullptr;
                }

                return ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *typeNode, ctx)});
            }
        }

        case NKikimrMiniKQL::ETypeKind::Tuple: {
            const auto& tupleType = type.GetTuple();
            if (tupleType.ElementSize() != value.TupleSize()) {
                ctx.AddError(TIssue(position, TStringBuilder() << "Bad tuple value, size mismatch"));
                return nullptr;
            }

            TExprNode::TListType itemNodes;
            for (ui32 i = 0; i < tupleType.ElementSize(); ++i) {
                const auto& itemType = tupleType.GetElement(i);
                auto itemNode = ParseKikimrProtoValue(itemType, value.GetTuple(i), pos, ctx);
                if (!itemNode) {
                    return nullptr;
                }

                itemNodes.push_back(itemNode);
            }

            return ctx.NewList(pos, std::move(itemNodes));
        }

        case NKikimrMiniKQL::ETypeKind::List: {
            const auto& itemType = type.GetList().GetItem();
            auto itemTypeNode = ParseTypeFromKikimrProto(itemType, ctx);
            if (!itemTypeNode) {
                return nullptr;
            }

            TExprNode::TListType itemNodes;
            for (ui32 i = 0; i < value.ListSize(); ++i) {
                auto itemNode = ParseKikimrProtoValue(itemType, value.GetList(i), pos, ctx);
                if (!itemNode) {
                    return nullptr;
                }

                itemNodes.push_back(itemNode);
            }

            return itemNodes.empty()
                ? ctx.NewCallable(pos, "List", {
                    ctx.NewCallable(pos, "ListType", {ExpandType(pos, *itemTypeNode, ctx)})})
                : ctx.NewCallable(pos, "AsList", std::move(itemNodes));
        }

        case NKikimrMiniKQL::ETypeKind::Struct: {
            const auto& structType = type.GetStruct();
            if (structType.MemberSize() != value.StructSize()) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Bad struct value, size mismatch"));
                return nullptr;
            }

            TExprNode::TListType structMembers;
            for (ui32 i = 0; i < structType.MemberSize(); ++i) {
                const auto& member = structType.GetMember(i);

                auto memberValueNode = ParseKikimrProtoValue(member.GetType(), value.GetStruct(i), pos, ctx);
                if (!memberValueNode) {
                    return nullptr;
                }

                auto memberNode = ctx.NewList(pos, {
                    ctx.NewAtom(pos, member.GetName()),
                    memberValueNode
                });

                structMembers.push_back(memberNode);
            }

            return ctx.NewCallable(pos, "AsStruct", std::move(structMembers));
        }

        case NKikimrMiniKQL::ETypeKind::Dict: {
            const auto& dictType = type.GetDict();
            TExprNode::TListType dictPairs;
            for (ui32 i = 0; i < value.DictSize(); ++i) {
                auto keyNode = ParseKikimrProtoValue(dictType.GetKey(), value.GetDict(i).GetKey(), pos, ctx);
                if (!keyNode) {
                    return nullptr;
                }

                auto payloadNode = ParseKikimrProtoValue(dictType.GetPayload(), value.GetDict(i).GetPayload(),
                    pos, ctx);
                if (!payloadNode) {
                    return nullptr;
                }

                auto pairNode = ctx.NewList(pos, {
                    keyNode,
                    payloadNode
                });

                dictPairs.push_back(pairNode);
            }

            return ctx.NewCallable(pos, "AsDict", std::move(dictPairs));
        }

        default: {
            ctx.AddError(TIssue(position, TStringBuilder() << "Unexpected type for protobuf value: " << type));
            return nullptr;
        }
    }
}

bool CheckKqpResultType(const NKikimrMiniKQL::TResult& kqpResult, const TTypeAnnotationNode& expectedType,
    TExprContext& ctx)
{
    YQL_ENSURE(kqpResult.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    const auto& structType = kqpResult.GetType().GetStruct();
    YQL_ENSURE(structType.GetMember(0).GetName() == "Data");
    YQL_ENSURE(structType.GetMember(1).GetName() == "Truncated");

    auto type = ParseTypeFromKikimrProto(structType.GetMember(0).GetType(), ctx);
    if (!type) {
        return false;
    }

    if (!expectedType.Equals(*type)) {
        ctx.AddError(TIssue(TPosition(), TStringBuilder() << "Kikimr result type mismatch, expected: "
            << expectedType << ", actual: " << *type));

        return false;
    }

    return true;
}

TMaybe<TString> KqpResultToYson(const NKikimrMiniKQL::TResult& kqpResult, const NYson::EYsonFormat& ysonFormat,
    TExprContext& ctx)
{
    YQL_ENSURE(kqpResult.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    const auto& structType = kqpResult.GetType().GetStruct();
    YQL_ENSURE(structType.GetMember(0).GetName() == "Data");
    YQL_ENSURE(structType.GetMember(1).GetName() == "Truncated");

    NKikimrMiniKQL::TResult dataResult;
    dataResult.MutableType()->CopyFrom(structType.GetMember(0).GetType());
    dataResult.MutableValue()->CopyFrom(kqpResult.GetValue().GetStruct(0));

    TStringStream out;
    NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, true);
    writer.OnBeginMap();
    writer.OnKeyedItem("Type");

    TVector<TString> columns;
    if (dataResult.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::List) {
        const auto& itemType = dataResult.GetType().GetList().GetItem();
        if (itemType.GetKind() == NKikimrMiniKQL::ETypeKind::Struct) {
            for (auto& member : itemType.GetStruct().GetMember()) {
                columns.push_back(member.GetName());
            }
        }
    }

    auto resultDataType = ParseTypeFromKikimrProto(dataResult.GetType(), ctx);
    if (!resultDataType) {
        return Nothing();
    }
    NCommon::WriteResOrPullType(writer, resultDataType, columns);

    writer.OnKeyedItem("Data");

    IDataProvider::TFillSettings fillSettings;
    fillSettings.AllResultsBytesLimit = Nothing();
    fillSettings.RowsLimitPerWrite = Nothing();
    fillSettings.Format = IDataProvider::EResultFormat::Yson;
    fillSettings.FormatDetails = ToString((ui32)ysonFormat);

    bool truncated;
    KikimrResultToYson(out, writer, dataResult, columns, fillSettings, truncated);
    YQL_ENSURE(!truncated);

    if (kqpResult.GetValue().GetStruct(1).GetBool()) {
        writer.OnKeyedItem("Truncated");
        writer.OnBooleanScalar(true);
    }

    writer.OnEndMap();
    return out.Str();
}

TMaybe<TString> GetTableListResult(const IKikimrGateway::TListPathResult& res,
    const IDataProvider::TFillSettings& fillSettings, TExprContext& ctx)
{
    NKikimrMiniKQL::TType stringType;
    stringType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    stringType.MutableData()->SetScheme(NKikimr::NUdf::TDataType<char*>::Id);

    NKikimrMiniKQL::TResult result;
    auto& structType = *result.MutableType();
    structType.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto& prefixMember = *structType.MutableStruct()->AddMember();
    prefixMember.SetName("Prefix");
    prefixMember.MutableType()->CopyFrom(stringType);
    auto& foldersMember = *structType.MutableStruct()->AddMember();
    foldersMember.SetName("Folders");
    foldersMember.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::List);
    foldersMember.MutableType()->MutableList()->MutableItem()->CopyFrom(stringType);
    auto& tablesMember = *structType.MutableStruct()->AddMember();
    tablesMember.SetName("Tables");
    tablesMember.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::List);
    tablesMember.MutableType()->MutableList()->MutableItem()->CopyFrom(stringType);

    auto& prefixValue = *result.MutableValue()->AddStruct();
    auto& foldersValue = *result.MutableValue()->AddStruct();
    auto& tablesValue = *result.MutableValue()->AddStruct();

    prefixValue.SetBytes(res.Path);

    for (const auto& item : res.Items) {
        if (item.IsDirectory) {
            foldersValue.AddList()->SetBytes(item.Name);
        } else {
            tablesValue.AddList()->SetBytes(item.Name);
        }
    }

    std::unique_ptr<NKikimrMiniKQL::TResult> packedResult(KikimrResultToProto(result, {}, fillSettings, nullptr));

    if (fillSettings.Format == IDataProvider::EResultFormat::Yson) {
        NYson::EYsonFormat ysonFormat = NCommon::GetYsonFormat(fillSettings);
        auto yson = KqpResultToYson(*packedResult, ysonFormat, ctx);
        if (!yson) {
            return Nothing();
        }

        return *yson;
    } else {
        YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Custom);
        YQL_ENSURE(fillSettings.FormatDetails == KikimrMkqlProtoFormat);

        TVector<char> buffer(packedResult->ByteSize());
        Y_PROTOBUF_SUPPRESS_NODISCARD packedResult->SerializeToArray(buffer.data(), buffer.size());
        return TString(buffer.data(), buffer.size());
    }
}


TMaybe<TString> GetTableMetadataResult(const TKikimrTableDescription& table,
    const IDataProvider::TFillSettings& fillSettings, TExprContext& ctx)
{
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Binary);
    table.ToYson(writer);

    auto metaYson = out.Str();

    NKikimrMiniKQL::TResult result;
    auto& resultType = *result.MutableType();
    resultType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    resultType.MutableData()->SetScheme(NKikimr::NUdf::TDataType<NKikimr::NUdf::TYson>::Id);

    result.MutableValue()->SetBytes(metaYson);

    std::unique_ptr<NKikimrMiniKQL::TResult> packedResult(KikimrResultToProto(result, {}, fillSettings, nullptr));

    if (fillSettings.Format == IDataProvider::EResultFormat::Yson) {
        NYson::EYsonFormat ysonFormat = NCommon::GetYsonFormat(fillSettings);
        auto yson = KqpResultToYson(*packedResult, ysonFormat, ctx);
        if (!yson) {
            return Nothing();
        }

        return *yson;
    } else {
        YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Custom);
        YQL_ENSURE(fillSettings.FormatDetails == KikimrMkqlProtoFormat);

        TVector<char> buffer(packedResult->ByteSize());
        Y_PROTOBUF_SUPPRESS_NODISCARD packedResult->SerializeToArray(buffer.data(), buffer.size());
        return TString(buffer.data(), buffer.size());
    }
}

void TransformerStatsToProto(const TString& name, const IGraphTransformer::TStatistics& stats,
    NKikimrKqp::TTransformProfile& profile)
{
    profile.SetName(name);
    if (stats.TransformDuration.MicroSeconds() > 0) {
        profile.SetTransformDurationUs(stats.TransformDuration.MicroSeconds());
    }
    if (stats.WaitDuration.MicroSeconds() > 0) {
        profile.SetWaitDurationUs(stats.WaitDuration.MicroSeconds());
    }
    if (stats.NewExprNodes > 0) {
        profile.SetNewExprNodes(stats.NewExprNodes);
    }
    if (stats.NewTypeNodes > 0) {
        profile.SetNewTypeNodes(stats.NewTypeNodes);
    }
    // TODO: stats.NewConstraintNodes
    if (stats.Repeats > 0) {
        profile.SetRepeats(stats.Repeats);
    }
    if (stats.Restarts > 0) {
        profile.SetRestarts(stats.Restarts);
    }

    for (auto& stage : stats.Stages) {
        TransformerStatsToProto(stage.first, stage.second, *profile.AddStages());
    }
}

void TransformerStatsFromProto(const NKikimrKqp::TTransformProfile& proto, IGraphTransformer::TStatistics& stats,
    TString& name)
{
    name = proto.GetName();

    stats.TransformDuration = TDuration::MicroSeconds(proto.GetTransformDurationUs());
    stats.WaitDuration = TDuration::MicroSeconds(proto.GetWaitDurationUs());
    stats.NewExprNodes = proto.GetNewExprNodes();
    stats.NewTypeNodes = proto.GetNewTypeNodes();
    stats.Repeats = proto.GetRepeats();
    stats.Restarts = proto.GetRestarts();

    for (auto& protoStage : proto.GetStages()) {
        TString stageName;
        IGraphTransformer::TStatistics stageStats;

        TransformerStatsFromProto(protoStage, stageStats, stageName);
        stats.Stages.emplace_back(stageName, stageStats);
    }
}

void KikimrReadOpStatsToYson(const NKikimrQueryStats::TReadOpStats& opStats, NYson::TYsonWriter& writer) {
    writer.OnBeginMap();
    writer.OnKeyedItem("Count");
    writer.OnUint64Scalar(opStats.GetCount());
    writer.OnKeyedItem("Rows");
    writer.OnUint64Scalar(opStats.GetRows());
    writer.OnKeyedItem("Bytes");
    writer.OnUint64Scalar(opStats.GetBytes());
    writer.OnEndMap();
}

void KikimrWriteOpStatsToYson(const NKikimrQueryStats::TWriteOpStats& opStats, NYson::TYsonWriter& writer) {
    writer.OnBeginMap();
    writer.OnKeyedItem("Count");
    writer.OnUint64Scalar(opStats.GetCount());
    writer.OnKeyedItem("Rows");
    writer.OnUint64Scalar(opStats.GetRows());
    writer.OnKeyedItem("Bytes");
    writer.OnUint64Scalar(opStats.GetBytes());
    writer.OnEndMap();
}

void KikimrTxStatsToYson(const NKikimrQueryStats::TTxStats& txStats, NYson::TYsonWriter& writer) {
    writer.OnBeginMap();
    writer.OnKeyedItem("TableAccessStats");

    writer.OnBeginList();
    for (auto& tableStats : txStats.GetTableAccessStats()) {
        writer.OnListItem();
        writer.OnBeginMap();

        writer.OnKeyedItem("TableInfo");
        {
            writer.OnBeginMap();
            writer.OnKeyedItem("SchemeshardId");
            writer.OnUint64Scalar(tableStats.GetTableInfo().GetSchemeshardId());
            writer.OnKeyedItem("PathId");
            writer.OnUint64Scalar(tableStats.GetTableInfo().GetPathId());
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(tableStats.GetTableInfo().GetName());
            writer.OnEndMap();
        }

        if (tableStats.HasSelectRow()) {
            writer.OnKeyedItem("SelectRow");
            KikimrReadOpStatsToYson(tableStats.GetSelectRow(), writer);
        }

        if (tableStats.HasSelectRange()) {
            writer.OnKeyedItem("SelectRange");
            KikimrReadOpStatsToYson(tableStats.GetSelectRange(), writer);
        }

        if (tableStats.HasUpdateRow()) {
            writer.OnKeyedItem("UpdateRow");
            KikimrWriteOpStatsToYson(tableStats.GetUpdateRow(), writer);
        }

        if (tableStats.HasEraseRow()) {
            writer.OnKeyedItem("EraseRow");
            KikimrWriteOpStatsToYson(tableStats.GetEraseRow(), writer);
        }

        writer.OnEndMap();
    }
    writer.OnEndList();

    if (txStats.HasDurationUs()) {
        writer.OnKeyedItem("DurationUs");
        writer.OnUint64Scalar(txStats.GetDurationUs());
    }

    writer.OnEndMap();
}

void KikimrProfileToYson(const NKikimrKqp::TKqlProfile& kqlProfile, NYson::TYsonWriter& writer) {
    writer.OnBeginMap();
    if (kqlProfile.HasQuery()) {
        writer.OnKeyedItem("Query");
        writer.OnStringScalar(kqlProfile.GetQuery());
    }

    writer.OnKeyedItem("MkqlProfiles");
    writer.OnBeginList();
    for (auto& mkql : kqlProfile.GetMkqlProfiles()) {
        writer.OnListItem();
        writer.OnBeginMap();
        writer.OnKeyedItem("Query");
        writer.OnStringScalar(mkql.HasQuery() ? mkql.GetQuery() : "");

        if (mkql.HasTxStats()) {
            writer.OnKeyedItem("TxStats");
            KikimrTxStatsToYson(mkql.GetTxStats(), writer);
        }

        writer.OnEndMap();
    }
    writer.OnEndList();

    if (kqlProfile.HasEffectiveIsolationLevel()) {
        writer.OnKeyedItem("EffectiveIsolationLevel");
        writer.OnStringScalar(NKikimrKqp::EIsolationLevel_Name(kqlProfile.GetEffectiveIsolationLevel()));
    }

    if (kqlProfile.HasTransformStats()) {
        writer.OnKeyedItem("TransformStats");

        TString transformName;
        IGraphTransformer::TStatistics transformStats;
        TransformerStatsFromProto(kqlProfile.GetTransformStats(), transformStats, transformName);
        NCommon::TransformerStatsToYson(transformName, transformStats, writer);
    }

    writer.OnEndMap();
}

void KikimrProfileToYson(const NKikimrKqp::TQueryProfile& queryProfile, NYson::TYsonWriter& writer) {
    writer.OnBeginMap();
    writer.OnKeyedItem("TransformStats");

    TString transformName;
    IGraphTransformer::TStatistics transformStats;
    TransformerStatsFromProto(queryProfile.GetTransformStats(), transformStats, transformName);
    NCommon::TransformerStatsToYson(transformName, transformStats, writer);

    writer.OnKeyedItem("KqlProfiles");
    writer.OnBeginList();
    for (auto& kqlProfile : queryProfile.GetKqlProfiles()) {
        writer.OnListItem();
        KikimrProfileToYson(kqlProfile, writer);
    }
    writer.OnEndList();
    writer.OnEndMap();
}

bool IsSameType(const NKikimrMiniKQL::TType& actual, const NKikimrMiniKQL::TType& expected) {
    if (actual.GetKind() != expected.GetKind()) {
        return false;
    }

    switch (actual.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void:
            return true;
        case NKikimrMiniKQL::ETypeKind::Data:
            return IsSameType(actual.GetData(), expected.GetData());
        case NKikimrMiniKQL::ETypeKind::Optional:
            return IsSameType(actual.GetOptional().GetItem(), expected.GetOptional().GetItem());
        case NKikimrMiniKQL::ETypeKind::List:
            return IsSameType(actual.GetList().GetItem(), expected.GetList().GetItem());
        case NKikimrMiniKQL::ETypeKind::Tuple:
            return IsSameType(actual.GetTuple(), expected.GetTuple());
        case NKikimrMiniKQL::ETypeKind::Struct:
            return IsSameType(actual.GetStruct(), expected.GetStruct());
        case NKikimrMiniKQL::ETypeKind::Dict:
            return IsSameType(actual.GetDict().GetKey(), expected.GetDict().GetKey()) &&
                IsSameType(actual.GetDict().GetPayload(), expected.GetDict().GetPayload());
        case NKikimrMiniKQL::ETypeKind::Variant:
            return IsSameType(actual.GetVariant(), expected.GetVariant());
        case NKikimrMiniKQL::ETypeKind::Null:
            return true;
        case NKikimrMiniKQL::ETypeKind::Pg:
            return actual.GetPg().Getoid() == expected.GetPg().Getoid();
        case NKikimrMiniKQL::ETypeKind::Unknown:
        case NKikimrMiniKQL::ETypeKind::Reserved_11:
        case NKikimrMiniKQL::ETypeKind::Reserved_12:
        case NKikimrMiniKQL::ETypeKind::Reserved_13:
        case NKikimrMiniKQL::ETypeKind::Reserved_14:
            return false;
    }
}

} // namespace NYql

