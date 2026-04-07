#include "yql_codec.h"
#include "yql_codec_type_flags.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/decimal/yql_decimal_serialize.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/public/result_format/yql_restricted_yson.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/swap_bytes.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/detail.h>

#include <util/string/cast.h>
#include <util/generic/map.h>

namespace NYql::NCommon {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYson::NDetail;

void WriteYsonValueImpl(NResult::TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, TType* type,
                        const TVector<ui32>* structPositions) {
    // Result format
    switch (type->GetKind()) {
        case TType::EKind::Void:
            writer.OnVoid();
            return;
        case TType::EKind::Null:
            writer.OnNull();
            return;
        case TType::EKind::EmptyList:
            writer.OnEmptyList();
            return;
        case TType::EKind::EmptyDict:
            writer.OnEmptyDict();
            return;
        case TType::EKind::Data: {
            auto dataType = AS_TYPE(TDataType, type);
            switch (*dataType->GetDataSlot()) {
                case NUdf::EDataSlot::Bool:
                    writer.OnBooleanScalar(value.Get<bool>());
                    return;
                case NUdf::EDataSlot::Int32:
                    writer.OnInt64Scalar(value.Get<i32>());
                    return;
                case NUdf::EDataSlot::Uint32:
                    writer.OnUint64Scalar(value.Get<ui32>());
                    return;
                case NUdf::EDataSlot::Int64:
                    writer.OnInt64Scalar(value.Get<i64>());
                    return;
                case NUdf::EDataSlot::Uint64:
                    writer.OnUint64Scalar(value.Get<ui64>());
                    return;
                case NUdf::EDataSlot::Uint8:
                    writer.OnUint64Scalar(value.Get<ui8>());
                    return;
                case NUdf::EDataSlot::Int8:
                    writer.OnInt64Scalar(value.Get<i8>());
                    return;
                case NUdf::EDataSlot::Uint16:
                    writer.OnUint64Scalar(value.Get<ui16>());
                    return;
                case NUdf::EDataSlot::Int16:
                    writer.OnInt64Scalar(value.Get<i16>());
                    return;
                case NUdf::EDataSlot::Float:
                    writer.OnFloatScalar(value.Get<float>());
                    return;
                case NUdf::EDataSlot::Double:
                    writer.OnDoubleScalar(value.Get<double>());
                    return;

                case NUdf::EDataSlot::Json:
                case NUdf::EDataSlot::Utf8:
                    // assume underlying string is utf8
                    writer.OnUtf8StringScalar(value.AsStringRef());
                    return;

                case NUdf::EDataSlot::String:
                case NUdf::EDataSlot::Uuid:
                    writer.OnStringScalar(value.AsStringRef());
                    return;

                case NUdf::EDataSlot::Decimal: {
                    const auto params = static_cast<TDataDecimalType*>(type)->GetParams();
                    const auto str = NDecimal::ToString(value.GetInt128(), params.first, params.second);
                    const auto size = str ? std::strlen(str) : 0;
                    writer.OnUtf8StringScalar(TStringBuf(str, size));
                    return;
                }
                case NUdf::EDataSlot::Yson:
                    NResult::EncodeRestrictedYson(writer, value.AsStringRef());
                    return;
                case NUdf::EDataSlot::Date:
                    writer.OnUint64Scalar(value.Get<ui16>());
                    return;
                case NUdf::EDataSlot::Datetime:
                    writer.OnUint64Scalar(value.Get<ui32>());
                    return;
                case NUdf::EDataSlot::Timestamp:
                    writer.OnUint64Scalar(value.Get<ui64>());
                    return;
                case NUdf::EDataSlot::Interval:
                    writer.OnInt64Scalar(value.Get<i64>());
                    return;
                case NUdf::EDataSlot::TzDate:
                case NUdf::EDataSlot::TzDatetime:
                case NUdf::EDataSlot::TzTimestamp:
                case NUdf::EDataSlot::TzDate32:
                case NUdf::EDataSlot::TzDatetime64:
                case NUdf::EDataSlot::TzTimestamp64:
                case NUdf::EDataSlot::JsonDocument:
                case NUdf::EDataSlot::DyNumber: {
                    const NUdf::TUnboxedValue out(ValueToString(*dataType->GetDataSlot(), value));
                    writer.OnUtf8StringScalar(out.AsStringRef());
                    return;
                }
                case NUdf::EDataSlot::Date32:
                    writer.OnInt64Scalar(value.Get<i32>());
                    return;
                case NUdf::EDataSlot::Datetime64:
                    writer.OnInt64Scalar(value.Get<i64>());
                    return;
                case NUdf::EDataSlot::Timestamp64:
                    writer.OnInt64Scalar(value.Get<i64>());
                    return;
                case NUdf::EDataSlot::Interval64:
                    writer.OnInt64Scalar(value.Get<i64>());
                    return;
            }
        } break;
        case TType::EKind::Pg: {
            auto pgType = AS_TYPE(TPgType, type);
            WriteYsonValuePg(writer, value, pgType, structPositions);
            return;
        }

        case TType::EKind::Struct: {
            writer.OnBeginList();
            auto structType = AS_TYPE(TStructType, type);
            if (structPositions && structPositions->size() != structType->GetMembersCount()) {
                YQL_ENSURE(false, "Invalid struct positions");
            }
            for (ui32 i = 0, e = structType->GetMembersCount(); i < e; ++i) {
                const ui32 pos = structPositions ? (*structPositions)[i] : i;
                if (pos < e) {
                    writer.OnListItem();
                    WriteYsonValueImpl(writer, value.GetElement(pos), structType->GetMemberType(pos), nullptr);
                }
            }

            writer.OnEndList();
            return;
        }
        case TType::EKind::List: {
            writer.OnBeginList();
            auto listType = AS_TYPE(TListType, type);
            const auto it = value.GetListIterator();
            for (NUdf::TUnboxedValue item; it.Next(item);) {
                writer.OnListItem();
                WriteYsonValueImpl(writer, item, listType->GetItemType(), nullptr);
            }

            writer.OnEndList();
            return;
        }
        case TType::EKind::Optional: {
            if (!value) {
                writer.OnEntity();
            } else {
                writer.OnBeginList();
                auto optionalType = AS_TYPE(TOptionalType, type);
                writer.OnListItem();
                WriteYsonValueImpl(writer, value.GetOptionalValue(), optionalType->GetItemType(), nullptr);
                writer.OnEndList();
            }
            return;
        }
        case TType::EKind::Dict: {
            writer.OnBeginList();
            auto dictType = AS_TYPE(TDictType, type);
            const auto it = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
                writer.OnListItem();
                writer.OnBeginList();
                {
                    writer.OnListItem();
                    WriteYsonValueImpl(writer, key, dictType->GetKeyType(), nullptr);
                    writer.OnListItem();
                    WriteYsonValueImpl(writer, payload, dictType->GetPayloadType(), nullptr);
                }
                writer.OnEndList();
            }

            writer.OnEndList();
        }
            return;
        case TType::EKind::Tuple: {
            writer.OnBeginList();
            auto tupleType = AS_TYPE(TTupleType, type);
            for (ui32 i = 0, e = tupleType->GetElementsCount(); i < e; ++i) {
                writer.OnListItem();
                WriteYsonValueImpl(writer, value.GetElement(i), tupleType->GetElementType(i), nullptr);
            }

            writer.OnEndList();
            return;
        }
        case TType::EKind::Variant: {
            writer.OnBeginList();
            auto underlyingType = AS_TYPE(TVariantType, type)->GetUnderlyingType();
            writer.OnListItem();
            auto index = value.GetVariantIndex();
            writer.OnUint64Scalar(index);
            writer.OnListItem();
            if (underlyingType->IsTuple()) {
                WriteYsonValueImpl(writer, value.GetVariantItem(), AS_TYPE(TTupleType, underlyingType)->GetElementType(index), nullptr);
            } else {
                WriteYsonValueImpl(writer, value.GetVariantItem(), AS_TYPE(TStructType, underlyingType)->GetMemberType(index), nullptr);
            }

            writer.OnEndList();
            return;
        }

        case TType::EKind::Tagged: {
            auto underlyingType = AS_TYPE(TTaggedType, type)->GetBaseType();
            WriteYsonValueImpl(writer, value, underlyingType, structPositions);
            return;
        }

        default:
            YQL_ENSURE(false, "unknown type " << type->GetKindAsStr());
    }
}

void WriteYsonValue(NYson::TYsonConsumerBase& writer, const NUdf::TUnboxedValuePod& value, TType* type,
                    const TVector<ui32>* structPositions)
{
    NResult::TYsonResultWriter resultWriter(writer);
    WriteYsonValueImpl(resultWriter, value, type, structPositions);
}

TString WriteYsonValue(const NUdf::TUnboxedValuePod& value, TType* type, const TVector<ui32>* structPositions,
                       NYson::EYsonFormat format) {
    TStringStream str;
    NYson::TYsonWriter writer(&str, format);
    WriteYsonValue(writer, value, type, structPositions);
    return str.Str();
}

TCodecContext::TCodecContext(
    const TTypeEnvironment& env,
    const IFunctionRegistry& functionRegistry,
    const NKikimr::NMiniKQL::THolderFactory* holderFactory /* = nullptr */
    )
    : Env(env)
    , Builder(Env, functionRegistry)
    , HolderFactory(holderFactory)
{
}

TMaybe<TVector<ui32>> CreateStructPositions(TType* inputType, const TVector<TString>* columns) {
    if (inputType->GetKind() != TType::EKind::Struct) {
        return Nothing();
    }

    auto inputStruct = AS_TYPE(TStructType, inputType);
    TMap<TStringBuf, ui32> members;
    TVector<ui32> structPositions(inputStruct->GetMembersCount(), Max<ui32>());
    for (ui32 i = 0; i < inputStruct->GetMembersCount(); ++i) {
        if (columns) {
            members.insert(std::make_pair(inputStruct->GetMemberName(i), i));
        } else {
            structPositions[i] = i;
        }
    }
    if (columns) {
        TColumnOrder order(*columns);
        ui32 pos = 0;
        for (auto& [column, gen_column] : order) {
            const ui32* idx = members.FindPtr(gen_column);
            YQL_ENSURE(idx, "Unknown member: " << gen_column);
            structPositions[pos] = *idx;
            ++pos;
        }
    }
    return structPositions;
}

namespace {
NYT::TNode DataValueToNode(const NKikimr::NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TType* type) {
    YQL_ENSURE(type->GetKind() == TType::EKind::Data);

    auto dataType = AS_TYPE(TDataType, type);
    switch (dataType->GetSchemeType()) {
        case NUdf::TDataType<i32>::Id:
            return NYT::TNode(value.Get<i32>());
        case NUdf::TDataType<i64>::Id:
            return NYT::TNode(value.Get<i64>());
        case NUdf::TDataType<ui32>::Id:
            return NYT::TNode(value.Get<ui32>());
        case NUdf::TDataType<ui64>::Id:
            return NYT::TNode(value.Get<ui64>());
        case NUdf::TDataType<float>::Id:
            return NYT::TNode(value.Get<float>());
        case NUdf::TDataType<double>::Id:
            return NYT::TNode(value.Get<double>());
        case NUdf::TDataType<bool>::Id:
            return NYT::TNode(value.Get<bool>());
        case NUdf::TDataType<ui8>::Id:
            return NYT::TNode((ui64)value.Get<ui8>());
        case NUdf::TDataType<i8>::Id:
            return NYT::TNode((i64)value.Get<i8>());
        case NUdf::TDataType<ui16>::Id:
            return NYT::TNode((ui64)value.Get<ui16>());
        case NUdf::TDataType<i16>::Id:
            return NYT::TNode((i64)value.Get<i16>());
        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id:
            return NYT::TNode(TString(value.AsStringRef()));
        case NUdf::TDataType<NUdf::TYson>::Id:
            return NYT::NodeFromYsonString(value.AsStringRef());
        case NUdf::TDataType<NUdf::TDate>::Id:
            return NYT::TNode((ui64)value.Get<ui16>());
        case NUdf::TDataType<NUdf::TDatetime>::Id:
            return NYT::TNode((ui64)value.Get<ui32>());
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            return NYT::TNode(value.Get<ui64>());
        case NUdf::TDataType<NUdf::TInterval>::Id:
            return NYT::TNode(value.Get<i64>());
        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            TStringStream out;
            out << value.Get<ui16>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            TStringStream out;
            out << value.Get<ui32>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            TStringStream out;
            out << value.Get<ui64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
        case NUdf::TDataType<NUdf::TDate32>::Id:
            return NYT::TNode((i64)value.Get<i32>());
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
            return NYT::TNode(value.Get<i64>());
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            return NYT::TNode(value.Get<i64>());
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            return NYT::TNode(value.Get<i64>());
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            const auto params = static_cast<NKikimr::NMiniKQL::TDataDecimalType*>(type)->GetParams();
            return NYT::TNode(NDecimal::ToString(value.GetInt128(), params.first, params.second));
        }
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            NUdf::TUnboxedValue json = ValueToString(EDataSlot::JsonDocument, value);
            return NYT::TNode(ToString(TStringBuf(value.AsStringRef())));
        }
        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            TStringStream out;
            out << value.Get<i32>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            TStringStream out;
            out << value.Get<i64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            TStringStream out;
            out << value.Get<i64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return NYT::TNode(out.Str());
        }
    }
    YQL_ENSURE(false, "Unsupported type: " << static_cast<int>(dataType->GetSchemeType()));
}

TExprNode::TPtr DataNodeToExprLiteral(TPositionHandle pos, const TTypeAnnotationNode& type, const NYT::TNode& node, TExprContext& ctx) {
    YQL_ENSURE(type.GetKind() == ETypeAnnotationKind::Data, "Expecting data type, got: " << type);

    TString strData;
    if (type.Cast<TDataExprType>()->GetSlot() == EDataSlot::Yson) {
        strData = NYT::NodeToYsonString(node);
    } else {
        switch (node.GetType()) {
            case NYT::TNode::String:
                strData = node.AsString();
                break;
            case NYT::TNode::Int64:
                strData = ToString(node.AsInt64());
                break;
            case NYT::TNode::Uint64:
                strData = ToString(node.AsUint64());
                break;
            case NYT::TNode::Double:
                strData = FloatToString(node.AsDouble());
                break;
            case NYT::TNode::Bool:
                strData = ToString(node.AsBool());
                break;
            default:
                YQL_ENSURE(false, "Unexpected Yson type: " << node.GetType() << " while deserializing literal of type " << type);
        }
    }

    return ctx.Builder(pos)
        .Callable(type.Cast<TDataExprType>()->GetName())
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            parent.Atom(0, strData);
            if (IsDataTypeDecimal(type.Cast<TDataExprType>()->GetSlot())) {
                auto decimalType = type.Cast<TDataExprParamsType>();
                parent.Atom(1, decimalType->GetParamOne());
                parent.Atom(2, decimalType->GetParamTwo());
            }
            return parent;
        })
        .Seal()
        .Build();
}

TString DataValueToString(const NKikimr::NUdf::TUnboxedValuePod& value, const TDataExprType* type) {
    switch (type->GetSlot()) {
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Date32:
            return ToString(value.Get<i32>());
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
            return ToString(value.Get<i64>());
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return ToString(value.Get<ui32>());
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return ToString(value.Get<ui64>());
        case NUdf::EDataSlot::Float:
            return ::FloatToString(value.Get<float>());
        case NUdf::EDataSlot::Double:
            return ::FloatToString(value.Get<double>());
        case NUdf::EDataSlot::Bool:
            return ToString(value.Get<bool>());
        case NUdf::EDataSlot::Uint8:
            return ToString(static_cast<unsigned int>(value.Get<ui8>()));
        case NUdf::EDataSlot::Int8:
            return ToString(static_cast<int>(value.Get<i8>()));
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return ToString(static_cast<unsigned int>(value.Get<ui16>()));
        case NUdf::EDataSlot::Int16:
            return ToString(static_cast<int>(value.Get<i16>()));
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::Yson:
            return ToString((TStringBuf)value.AsStringRef());
        case NUdf::EDataSlot::Decimal: {
            const auto params = dynamic_cast<const TDataExprParamsType*>(type);
            YQL_ENSURE(params, "Unable to cast decimal params");
            return NDecimal::ToString(value.GetInt128(), FromString<ui8>(params->GetParamOne()), FromString<ui8>(params->GetParamTwo()));
        }
        case NUdf::EDataSlot::TzDate: {
            TStringStream out;
            out << value.Get<ui16>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }
        case NUdf::EDataSlot::TzDatetime: {
            TStringStream out;
            out << value.Get<ui32>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }
        case NUdf::EDataSlot::TzTimestamp: {
            TStringStream out;
            out << value.Get<ui64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }
        case NUdf::EDataSlot::TzDate32: {
            TStringStream out;
            out << value.Get<i32>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }
        case NUdf::EDataSlot::TzDatetime64: {
            TStringStream out;
            out << value.Get<i64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }
        case NUdf::EDataSlot::TzTimestamp64: {
            TStringStream out;
            out << value.Get<i64>() << "," << NKikimr::NMiniKQL::GetTimezoneIANAName(value.GetTimezoneId());
            return out.Str();
        }

        case NUdf::EDataSlot::JsonDocument: {
            NUdf::TUnboxedValue json = ValueToString(EDataSlot::JsonDocument, value);
            return ToString(TStringBuf(json.AsStringRef()));
        }

        case NUdf::EDataSlot::DyNumber: {
            NUdf::TUnboxedValue res = ValueToString(EDataSlot::DyNumber, value);
            return ToString(TStringBuf(res.AsStringRef()));
        }
    }

    Y_ABORT("Unexpected");
}
} // namespace

NYT::TNode ValueToNode(const NKikimr::NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TType* type) {
    NYT::TNode result;
    switch (type->GetKind()) {
        case TType::EKind::Optional: {
            result = NYT::TNode::CreateList();
            if (value) {
                result.Add(ValueToNode(value.GetOptionalValue(), AS_TYPE(TOptionalType, type)->GetItemType()));
            }
            break;
        }
        case TType::EKind::Tuple: {
            auto tupleType = AS_TYPE(TTupleType, type);
            result = NYT::TNode::CreateList();
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                result.Add(ValueToNode(value.GetElement(i), tupleType->GetElementType(i)));
            }
            break;
        }
        case TType::EKind::List: {
            auto listType = AS_TYPE(TListType, type);
            result = NYT::TNode::CreateList();
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                result.Add(ValueToNode(item, listType->GetItemType()));
            }
            break;
        }
        default: {
            result = DataValueToNode(value, type);
        }
    }
    return result;
}

TExprNode::TPtr NodeToExprLiteral(TPositionHandle pos, const TTypeAnnotationNode& type, const NYT::TNode& node, TExprContext& ctx) {
    TExprNode::TPtr result;
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Optional: {
            YQL_ENSURE(node.IsList() || node.IsNull());
            if (node.IsNull() || node.AsList().empty()) {
                return ctx.NewCallable(pos, "Nothing", {ExpandType(pos, type, ctx)});
            }
            YQL_ENSURE(node.AsList().size() == 1);
            result = ctx.NewCallable(pos, "Just", {NodeToExprLiteral(pos, *type.Cast<TOptionalExprType>()->GetItemType(), node.AsList().front(), ctx)});
            break;
        }
        case ETypeAnnotationKind::Tuple: {
            YQL_ENSURE(node.IsList());
            const TTypeAnnotationNode::TListType& itemTypes = type.Cast<TTupleExprType>()->GetItems();
            const auto& items = node.AsList();
            YQL_ENSURE(itemTypes.size() == items.size());
            TExprNodeList resultNodes;
            for (size_t i = 0; i < items.size(); ++i) {
                resultNodes.push_back(NodeToExprLiteral(pos, *itemTypes[i], items[i], ctx));
            }
            result = ctx.NewList(pos, std::move(resultNodes));
            break;
        }
        case ETypeAnnotationKind::List: {
            YQL_ENSURE(node.IsList());
            const TTypeAnnotationNode& itemType = *type.Cast<TListExprType>()->GetItemType();
            if (node.AsList().empty()) {
                return ctx.NewCallable(pos, "List", {ExpandType(pos, *ctx.MakeType<TListExprType>(&itemType), ctx)});
            }

            TExprNodeList children;
            for (auto& child : node.AsList()) {
                children.push_back(NodeToExprLiteral(pos, itemType, child, ctx));
            }

            result = ctx.NewCallable(pos, "AsList", std::move(children));
            break;
        }
        default: {
            result = DataNodeToExprLiteral(pos, type, node, ctx);
        }
    }
    return result;
}

void CopyYsonWithAttrs(char cmd, TInputBuf& buf, TVector<char>& yson) {
    if (cmd == BeginAttributesSymbol) {
        yson.push_back(cmd);
        cmd = buf.Read();

        for (;;) {
            if (cmd == EndAttributesSymbol) {
                yson.push_back(cmd);
                cmd = buf.Read();
                break;
            }

            CHECK_EXPECTED(cmd, StringMarker);
            yson.push_back(cmd);
            i32 length = buf.CopyVarI32(yson);
            CHECK_STRING_LENGTH(length);
            buf.CopyMany(length, yson);
            EXPECTED_COPY(buf, KeyValueSeparatorSymbol, yson);
            cmd = buf.Read();
            CopyYsonWithAttrs(cmd, buf, yson);
            cmd = buf.Read();
            if (cmd == KeyedItemSeparatorSymbol) {
                yson.push_back(cmd);
                cmd = buf.Read();
            }
        }
    }

    CopyYson(cmd, buf, yson);
}

void CopyYson(char cmd, TInputBuf& buf, TVector<char>& yson) {
    switch (cmd) {
        case EntitySymbol:
        case TrueMarker:
        case FalseMarker:
            yson.push_back(cmd);
            break;
        case Int64Marker:
            yson.push_back(cmd);
            buf.CopyVarI64(yson);
            break;
        case Uint64Marker:
            yson.push_back(cmd);
            buf.CopyVarUI64(yson);
            break;
        case DoubleMarker:
            yson.push_back(cmd);
            buf.CopyMany(8, yson);
            break;
        case StringMarker: {
            yson.push_back(cmd);
            i32 length = buf.CopyVarI32(yson);
            CHECK_STRING_LENGTH(length);
            buf.CopyMany(length, yson);
            break;
        }
        case BeginListSymbol: {
            yson.push_back(cmd);
            cmd = buf.Read();

            for (;;) {
                if (cmd == EndListSymbol) {
                    yson.push_back(cmd);
                    break;
                }

                CopyYsonWithAttrs(cmd, buf, yson);
                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    yson.push_back(cmd);
                    cmd = buf.Read();
                }
            }
            break;
        }
        case BeginMapSymbol: {
            yson.push_back(cmd);
            cmd = buf.Read();

            for (;;) {
                if (cmd == EndMapSymbol) {
                    yson.push_back(cmd);
                    break;
                }

                CHECK_EXPECTED(cmd, StringMarker);
                yson.push_back(cmd);
                i32 length = buf.CopyVarI32(yson);
                CHECK_STRING_LENGTH(length);
                buf.CopyMany(length, yson);
                EXPECTED_COPY(buf, KeyValueSeparatorSymbol, yson);
                cmd = buf.Read();
                CopyYsonWithAttrs(cmd, buf, yson);
                cmd = buf.Read();
                if (cmd == KeyedItemSeparatorSymbol) {
                    yson.push_back(cmd);
                    cmd = buf.Read();
                }
            }
            break;
        }
        default:
            YQL_ENSURE(false, "Unexpected yson character: " << cmd);
    }
}

void SkipYson(char cmd, TInputBuf& buf) {
    auto& yson = buf.YsonBuffer();
    yson.clear();
    CopyYsonWithAttrs(cmd, buf, yson);
}

NUdf::TUnboxedValue ReadYsonStringInResultFormat(char cmd, TInputBuf& buf) {
    NUdf::TUnboxedValue result;
    const bool needDecode = (cmd == BeginListSymbol);

    if (needDecode) {
        cmd = buf.Read();
    }

    CHECK_EXPECTED(cmd, StringMarker);
    const i32 length = buf.ReadVarI32();
    CHECK_STRING_LENGTH(length);
    TTempBuf tmpBuf(length);
    buf.ReadMany(tmpBuf.Data(), length);

    if (needDecode) {
        TString decoded = Base64Decode(TStringBuf(tmpBuf.Data(), length));
        result = NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(decoded)));
    } else {
        result = NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(tmpBuf.Data(), length)));
    }

    if (needDecode) {
        cmd = buf.Read();
        if (cmd == ListItemSeparatorSymbol) {
            cmd = buf.Read();
        }

        CHECK_EXPECTED(cmd, EndListSymbol);
    }
    return result;
}

TStringBuf ReadNextString(char cmd, TInputBuf& buf) {
    CHECK_EXPECTED(cmd, StringMarker);
    return buf.ReadYtString();
}

template <typename T>
T ReadNextSerializedNumber(char cmd, TInputBuf& buf) {
    auto nextString = ReadNextString(cmd, buf);
    if constexpr (!std::numeric_limits<T>::is_integer) {
        if (nextString == "inf" || nextString == "+inf") {
            return std::numeric_limits<T>::infinity();
        } else if (nextString == "-inf") {
            return -std::numeric_limits<T>::infinity();
        } else if (nextString == "nan") {
            return std::numeric_limits<T>::quiet_NaN();
        }
    }
    return FromString<T>(nextString);
}

template <typename T>
T ReadYsonFloatNumber(char cmd, TInputBuf& buf) {
    return ReadNextSerializedNumber<T>(cmd, buf);
}

NUdf::TUnboxedValue ReadYsonValue(TType* type,
                                  const NKikimr::NMiniKQL::THolderFactory& holderFactory, char cmd, TInputBuf& buf) {
    switch (type->GetKind()) {
        case TType::EKind::Variant: {
            auto varType = static_cast<TVariantType*>(type);
            auto underlyingType = varType->GetUnderlyingType();
            if (cmd == StringMarker) {
                YQL_ENSURE(underlyingType->IsStruct(), "Expected struct as underlying type");
                auto name = ReadNextString(cmd, buf);
                auto index = static_cast<TStructType*>(underlyingType)->FindMemberIndex(name);
                YQL_ENSURE(index, "Unexpected member: " << name);
                YQL_ENSURE(static_cast<TStructType*>(underlyingType)->GetMemberType(*index)->IsVoid(), "Expected Void as underlying type");
                return holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod::Zero(), *index);
            }

            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();
            i64 index = 0;
            if (cmd == BeginListSymbol) {
                cmd = buf.Read();
                YQL_ENSURE(underlyingType->IsStruct(), "Expected struct as underlying type");
                auto name = ReadNextString(cmd, buf);
                auto foundIndex = static_cast<TStructType*>(underlyingType)->FindMemberIndex(name);
                YQL_ENSURE(foundIndex, "Unexpected member: " << name);
                index = *foundIndex;
                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = buf.Read();
                }

                CHECK_EXPECTED(cmd, EndListSymbol);
            } else {
                index = ReadNextSerializedNumber<ui64>(cmd, buf);
            }

            YQL_ENSURE(index < varType->GetAlternativesCount(), "Bad variant alternative: " << index << ", only " << varType->GetAlternativesCount() << " are available");
            YQL_ENSURE(underlyingType->IsTuple() || underlyingType->IsStruct(), "Wrong underlying type");
            TType* itemType;
            if (underlyingType->IsTuple()) {
                itemType = static_cast<TTupleType*>(underlyingType)->GetElementType(index);
            } else {
                itemType = static_cast<TStructType*>(underlyingType)->GetMemberType(index);
            }

            EXPECTED(buf, ListItemSeparatorSymbol);
            cmd = buf.Read();
            auto value = ReadYsonValue(itemType, holderFactory, cmd, buf);
            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return holderFactory.CreateVariantHolder(value.Release(), index);
        }

        case TType::EKind::Data: {
            auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
            switch (schemeType) {
                case NUdf::TDataType<bool>::Id:
                    YQL_ENSURE(cmd == FalseMarker || cmd == TrueMarker, "Expected either true or false, but got: " << TString(cmd).Quote());
                    return NUdf::TUnboxedValuePod(cmd == TrueMarker);

                case NUdf::TDataType<ui8>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui8>(cmd, buf));

                case NUdf::TDataType<i8>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i8>(cmd, buf));

                case NUdf::TDataType<ui16>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui16>(cmd, buf));

                case NUdf::TDataType<i16>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i16>(cmd, buf));

                case NUdf::TDataType<i32>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i32>(cmd, buf));

                case NUdf::TDataType<ui32>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui32>(cmd, buf));

                case NUdf::TDataType<i64>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i64>(cmd, buf));

                case NUdf::TDataType<ui64>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui64>(cmd, buf));

                case NUdf::TDataType<float>::Id:
                    return NUdf::TUnboxedValuePod(ReadYsonFloatNumber<float>(cmd, buf));

                case NUdf::TDataType<double>::Id:
                    return NUdf::TUnboxedValuePod(ReadYsonFloatNumber<double>(cmd, buf));

                case NUdf::TDataType<NUdf::TUtf8>::Id:
                case NUdf::TDataType<char*>::Id:
                case NUdf::TDataType<NUdf::TJson>::Id:
                case NUdf::TDataType<NUdf::TUuid>::Id: {
                    return ReadYsonStringInResultFormat(cmd, buf);
                }

                case NUdf::TDataType<NUdf::TDecimal>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    const auto params = static_cast<TDataDecimalType*>(type)->GetParams();
                    const auto val = NDecimal::FromString(nextString, params.first, params.second);
                    YQL_ENSURE(!NDecimal::IsError(val));
                    return NUdf::TUnboxedValuePod(val);
                }

                case NUdf::TDataType<NUdf::TYson>::Id: {
                    auto& yson = buf.YsonBuffer();
                    yson.clear();
                    CopyYsonWithAttrs(cmd, buf, yson);

                    TString decodedYson = NResult::DecodeRestrictedYson(TStringBuf(yson.data(), yson.size()), NYson::EYsonFormat::Text);
                    return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(decodedYson)));
                }

                case NUdf::TDataType<NUdf::TDate>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui16>(cmd, buf));

                case NUdf::TDataType<NUdf::TDatetime>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui32>(cmd, buf));

                case NUdf::TDataType<NUdf::TTimestamp>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<ui64>(cmd, buf));

                case NUdf::TDataType<NUdf::TInterval>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i64>(cmd, buf));

                case NUdf::TDataType<NUdf::TTzDate>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzDate, nextString);
                    YQL_ENSURE(data, "incorrect tz date format for value " << nextString);
                    return data;
                }

                case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzDatetime, nextString);
                    YQL_ENSURE(data, "incorrect tz datetime format for value " << nextString);
                    return data;
                }

                case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzTimestamp, nextString);
                    YQL_ENSURE(data, "incorrect tz timestamp format for value " << nextString);
                    return data;
                }

                case NUdf::TDataType<NUdf::TDate32>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i32>(cmd, buf));

                case NUdf::TDataType<NUdf::TDatetime64>::Id:
                case NUdf::TDataType<NUdf::TTimestamp64>::Id:
                case NUdf::TDataType<NUdf::TInterval64>::Id:
                    return NUdf::TUnboxedValuePod(ReadNextSerializedNumber<i64>(cmd, buf));

                case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
                    const auto json = ReadYsonStringInResultFormat(cmd, buf);
                    return ValueFromString(EDataSlot::JsonDocument, json.AsStringRef());
                }

                case NUdf::TDataType<NUdf::TDyNumber>::Id: {
                    const auto str = ReadYsonStringInResultFormat(cmd, buf);
                    return ValueFromString(EDataSlot::DyNumber, str.AsStringRef());
                }

                case NUdf::TDataType<NUdf::TTzDate32>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzDate32, nextString);
                    YQL_ENSURE(data, "incorrect tz date format for value " << nextString);
                    return data;
                }

                case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzDatetime64, nextString);
                    YQL_ENSURE(data, "incorrect tz datetime format for value " << nextString);
                    return data;
                }

                case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
                    auto nextString = ReadNextString(cmd, buf);
                    NUdf::TUnboxedValuePod data;
                    data = ValueFromString(NUdf::EDataSlot::TzTimestamp64, nextString);
                    YQL_ENSURE(data, "incorrect tz timestamp format for value " << nextString);
                    return data;
                }

                default:
                    YQL_ENSURE(false, "Unsupported data type: " << schemeType);
            }
        }

        case TType::EKind::Struct: {
            YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol);
            auto structType = static_cast<TStructType*>(type);
            NUdf::TUnboxedValue* items;
            NUdf::TUnboxedValue ret = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);
            if (cmd == BeginListSymbol) {
                cmd = buf.Read();

                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    items[i] = ReadYsonValue(structType->GetMemberType(i), holderFactory, cmd, buf);

                    cmd = buf.Read();
                    if (cmd == ListItemSeparatorSymbol) {
                        cmd = buf.Read();
                    }
                }

                CHECK_EXPECTED(cmd, EndListSymbol);
                return ret;
            } else {
                cmd = buf.Read();

                for (;;) {
                    if (cmd == EndMapSymbol) {
                        break;
                    }

                    auto keyBuffer = ReadNextString(cmd, buf);
                    auto pos = structType->FindMemberIndex(keyBuffer);
                    EXPECTED(buf, KeyValueSeparatorSymbol);
                    cmd = buf.Read();
                    if (pos && cmd != '#') {
                        auto memberType = structType->GetMemberType(*pos);
                        auto unwrappedType = memberType;
                        items[*pos] = ReadYsonValue(unwrappedType, holderFactory, cmd, buf);
                    } else {
                        SkipYson(cmd, buf);
                    }

                    cmd = buf.Read();
                    if (cmd == KeyedItemSeparatorSymbol) {
                        cmd = buf.Read();
                    }
                }

                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    if (items[i]) {
                        continue;
                    }

                    YQL_ENSURE(structType->GetMemberType(i)->IsOptional(), "Missing required field: " << structType->GetMemberName(i));
                }

                return ret;
            }
        }

        case TType::EKind::List: {
            auto itemType = static_cast<TListType*>(type)->GetItemType();
            TDefaultListRepresentation items;
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();

            for (;;) {
                if (cmd == EndListSymbol) {
                    break;
                }

                items = items.Append(ReadYsonValue(itemType, holderFactory, cmd, buf));

                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = buf.Read();
                }
            }

            return holderFactory.CreateDirectListHolder(std::move(items));
        }

        case TType::EKind::Optional: {
            if (cmd == EntitySymbol) {
                return NUdf::TUnboxedValuePod();
            }
            auto itemType = static_cast<TOptionalType*>(type)->GetItemType();
            if (cmd != BeginListSymbol) {
                auto value = ReadYsonValue(itemType, holderFactory, cmd, buf);
                return value.Release().MakeOptional();
            }

            cmd = buf.Read();
            if (cmd == EndListSymbol) {
                return NUdf::TUnboxedValuePod();
            }

            auto value = ReadYsonValue(itemType, holderFactory, cmd, buf);
            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return value.Release().MakeOptional();
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            TKeyTypes types;
            bool isTuple;
            bool encoded;
            bool useIHash;
            GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);

            TMaybe<TValuePacker> packer;
            if (encoded) {
                packer.ConstructInPlace(true, keyType);
            }

            YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol, "Expected '{' or '[', but read: " << TString(cmd).Quote());
            if (cmd == BeginMapSymbol) {
                bool unusedIsOptional;
                auto unpackedType = UnpackOptional(keyType, unusedIsOptional);
                YQL_ENSURE(unpackedType->IsData() &&
                               (static_cast<TDataType*>(unpackedType)->GetSchemeType() == NUdf::TDataType<char*>::Id ||
                                static_cast<TDataType*>(unpackedType)->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id),
                           "Expected String or Utf8 type as dictionary key type");

                auto filler = [&](TValuesDictHashMap& map) {
                    cmd = buf.Read();

                    for (;;) {
                        if (cmd == EndMapSymbol) {
                            break;
                        }

                        auto keyBuffer = ReadNextString(cmd, buf);
                        auto keyStr = NUdf::TUnboxedValue(MakeString(keyBuffer));
                        EXPECTED(buf, KeyValueSeparatorSymbol);
                        cmd = buf.Read();
                        auto payload = ReadYsonValue(payloadType, holderFactory, cmd, buf);
                        map.emplace(std::move(keyStr), std::move(payload));

                        cmd = buf.Read();
                        if (cmd == KeyedItemSeparatorSymbol) {
                            cmd = buf.Read();
                        }
                    }
                };

                const NUdf::IHash* hash = holderFactory.GetHash(*keyType, useIHash);
                const NUdf::IEquate* equate = holderFactory.GetEquate(*keyType, useIHash);
                return holderFactory.CreateDirectHashedDictHolder(filler, types, isTuple, true, nullptr, hash, equate);
            } else {
                auto filler = [&](TValuesDictHashMap& map) {
                    cmd = buf.Read();

                    for (;;) {
                        if (cmd == EndListSymbol) {
                            break;
                        }

                        CHECK_EXPECTED(cmd, BeginListSymbol);
                        cmd = buf.Read();
                        auto key = ReadYsonValue(keyType, holderFactory, cmd, buf);
                        EXPECTED(buf, ListItemSeparatorSymbol);
                        cmd = buf.Read();
                        auto payload = ReadYsonValue(payloadType, holderFactory, cmd, buf);
                        cmd = buf.Read();
                        if (cmd == ListItemSeparatorSymbol) {
                            cmd = buf.Read();
                        }

                        CHECK_EXPECTED(cmd, EndListSymbol);
                        if (packer) {
                            key = MakeString(packer->Pack(key));
                        }

                        map.emplace(std::move(key), std::move(payload));

                        cmd = buf.Read();
                        if (cmd == ListItemSeparatorSymbol) {
                            cmd = buf.Read();
                        }
                    }
                };

                const NUdf::IHash* hash = holderFactory.GetHash(*keyType, useIHash);
                const NUdf::IEquate* equate = holderFactory.GetEquate(*keyType, useIHash);
                return holderFactory.CreateDirectHashedDictHolder(filler, types, isTuple, true, encoded ? keyType : nullptr,
                                                                  hash, equate);
            }
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<TTupleType*>(type);
            NUdf::TUnboxedValue* items;
            NUdf::TUnboxedValue ret = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();

            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                items[i] = ReadYsonValue(tupleType->GetElementType(i), holderFactory, cmd, buf);

                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = buf.Read();
                }
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return ret;
        }

        case TType::EKind::Void: {
            if (cmd == EntitySymbol) {
                return NUdf::TUnboxedValuePod::Void();
            }

            auto nextString = ReadNextString(cmd, buf);
            YQL_ENSURE(nextString == NResult::TYsonResultWriter::VoidString, "Expected Void");
            return NUdf::TUnboxedValuePod::Void();
        }

        case TType::EKind::Null: {
            CHECK_EXPECTED(cmd, EntitySymbol);
            return NUdf::TUnboxedValuePod();
        }

        case TType::EKind::EmptyList: {
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();
            CHECK_EXPECTED(cmd, EndListSymbol);
            return holderFactory.GetEmptyContainerLazy();
        }

        case TType::EKind::EmptyDict: {
            YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol, "Expected '{' or '[', but read: " << TString(cmd).Quote());
            if (cmd == BeginListSymbol) {
                cmd = buf.Read();
                CHECK_EXPECTED(cmd, EndListSymbol);
            } else {
                cmd = buf.Read();
                CHECK_EXPECTED(cmd, EndMapSymbol);
            }

            return holderFactory.GetEmptyContainerLazy();
        }

        case TType::EKind::Pg: {
            auto pgType = static_cast<TPgType*>(type);
            return ReadYsonValuePg(pgType, cmd, buf);
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<TTaggedType*>(type);
            return ReadYsonValue(taggedType->GetBaseType(), holderFactory, cmd, buf);
        }

        default:
            YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

TMaybe<NUdf::TUnboxedValue> ParseYsonValue(const THolderFactory& holderFactory,
                                           const TStringBuf& yson, TType* type, IOutputStream* err) {
    try {
        class TReader: public IBlockReader {
        public:
            explicit TReader(const TStringBuf& yson)
                : Yson_(yson)
            {
            }

            void SetDeadline(TInstant deadline) override {
                Y_UNUSED(deadline);
            }

            std::pair<const char*, const char*> NextFilledBlock() override {
                if (FirstBuffer_) {
                    FirstBuffer_ = false;
                    return {Yson_.begin(), Yson_.end()};
                } else {
                    return {nullptr, nullptr};
                }
            }

            void ReturnBlock() override {
            }

            bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override {
                Y_UNUSED(rangeIndex);
                Y_UNUSED(rowIndex);
                Y_UNUSED(error);
                return false;
            }

        private:
            TStringBuf Yson_;
            bool FirstBuffer_ = true;
        };

        TReader reader(yson);
        TInputBuf buf(reader, nullptr);
        char cmd = buf.Read();
        return ReadYsonValue(type, holderFactory, cmd, buf);
    } catch (const yexception& e) {
        if (err) {
            *err << "YSON parsing failed: " << e.what();
        }
        return Nothing();
    }
}

TMaybe<NUdf::TUnboxedValue> ParseYsonNodeInResultFormat(const THolderFactory& holderFactory,
                                                        const NYT::TNode& node, TType* type, IOutputStream* err) {
    return ParseYsonValue(holderFactory, NYT::NodeToYsonString(node, NYson::EYsonFormat::Binary), type, err);
}

TExprNode::TPtr ValueToExprLiteral(const TTypeAnnotationNode* type, const NKikimr::NUdf::TUnboxedValuePod& value, TExprContext& ctx,
                                   TPositionHandle pos) {
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Variant: {
            auto variantType = type->Cast<TVariantExprType>();
            ui32 index = value.GetVariantIndex();
            const TTypeAnnotationNode* itemType;
            if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct) {
                // struct
                const auto& items = variantType->GetUnderlyingType()->Cast<TStructExprType>()->GetItems();
                YQL_ENSURE(index < items.size());
                itemType = items[index]->GetItemType();
            } else if (variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                // tuple
                const auto& items = variantType->GetUnderlyingType()->Cast<TTupleExprType>()->GetItems();
                YQL_ENSURE(index < items.size());
                itemType = items[index];
            } else {
                YQL_ENSURE(false, "Unknown underlying type");
            }

            return ctx.NewCallable(pos, "Variant", {ValueToExprLiteral(itemType, value.GetVariantItem(), ctx, pos),
                                                    ctx.NewAtom(pos, variantType->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Struct ? variantType->GetUnderlyingType()->Cast<TStructExprType>()->GetItems()[index]->GetName() : ToString(index)),
                                                    ExpandType(pos, *type, ctx)});
        }

        case ETypeAnnotationKind::Data: {
            auto dataType = type->Cast<TDataExprType>();
            TVector<TExprNode::TPtr> args({ctx.NewAtom(pos, DataValueToString(value, dataType))});
            if (auto params = dynamic_cast<const TDataExprParamsType*>(dataType)) {
                args.reserve(3);
                args.push_back(ctx.NewAtom(pos, params->GetParamOne()));
                args.push_back(ctx.NewAtom(pos, params->GetParamTwo()));
            }

            return ctx.NewCallable(pos, dataType->GetName(), std::move(args));
        }

        case ETypeAnnotationKind::Struct: {
            auto structType = type->Cast<TStructExprType>();
            TExprNode::TListType items;
            items.reserve(1 + structType->GetSize());
            items.emplace_back(ExpandType(pos, *type, ctx));
            for (ui32 i = 0; i < structType->GetSize(); ++i) {
                auto pair = ctx.NewList(pos, {ctx.NewAtom(pos, structType->GetItems()[i]->GetName()),
                                              ValueToExprLiteral(structType->GetItems()[i]->GetItemType(), value.GetElement(i), ctx, pos)});

                items.emplace_back(std::move(pair));
            }

            return ctx.NewCallable(pos, "Struct", std::move(items));
        }

        case ETypeAnnotationKind::List: {
            auto listType = type->Cast<TListExprType>();
            auto itemType = listType->GetItemType();
            TExprNode::TListType items;
            items.emplace_back(ExpandType(pos, *type, ctx));
            NUdf::TUnboxedValue itemValue;
            for (auto iter = value.GetListIterator(); iter.Next(itemValue);) {
                items.emplace_back(ValueToExprLiteral(itemType, itemValue, ctx, pos));
            }

            if (items.size() > 1) {
                items.erase(items.begin());
                return ctx.NewCallable(pos, "AsList", std::move(items));
            }

            return ctx.NewCallable(pos, "List", std::move(items));
        }

        case ETypeAnnotationKind::Optional: {
            auto optionalType = type->Cast<TOptionalExprType>();
            auto itemType = optionalType->GetItemType();
            if (!value) {
                return ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *type, ctx)});
            } else {
                return ctx.NewCallable(pos, "Just", {ValueToExprLiteral(itemType, value.GetOptionalValue(), ctx, pos)});
            }
        }

        case ETypeAnnotationKind::Dict: {
            auto dictType = type->Cast<TDictExprType>();
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();
            TExprNode::TListType items;
            items.emplace_back(ExpandType(pos, *type, ctx));
            NUdf::TUnboxedValue keyValue, payloadValue;
            for (auto iter = value.GetDictIterator(); iter.NextPair(keyValue, payloadValue);) {
                auto pair = ctx.NewList(pos, {ValueToExprLiteral(keyType, keyValue, ctx, pos),
                                              ValueToExprLiteral(payloadType, payloadValue, ctx, pos)});

                items.emplace_back(std::move(pair));
            }

            return ctx.NewCallable(pos, "Dict", std::move(items));
        }

        case ETypeAnnotationKind::Tuple: {
            auto tupleType = type->Cast<TTupleExprType>();
            TExprNode::TListType items;
            items.reserve(tupleType->GetSize());
            for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
                items.emplace_back(ValueToExprLiteral(tupleType->GetItems()[i], value.GetElement(i), ctx, pos));
            }

            return ctx.NewList(pos, std::move(items));
        }

        case ETypeAnnotationKind::Void: {
            return ctx.NewCallable(pos, "Void", {});
        }

        case ETypeAnnotationKind::Null: {
            return ctx.NewCallable(pos, "Null", {});
        }

        case ETypeAnnotationKind::EmptyList: {
            return ctx.NewCallable(pos, "AsList", {});
        }

        case ETypeAnnotationKind::EmptyDict: {
            return ctx.NewCallable(pos, "AsDict", {});
        }

        case ETypeAnnotationKind::Tagged: {
            auto taggedType = type->Cast<TTaggedExprType>();
            auto baseType = taggedType->GetBaseType();
            return ctx.NewCallable(pos, "AsTagged", {
                                                        ValueToExprLiteral(baseType, value, ctx, pos),
                                                        ctx.NewAtom(pos, taggedType->GetTag()),
                                                    });
        }

        case ETypeAnnotationKind::Pg: {
            auto pgType = type->Cast<TPgExprType>();
            if (!value) {
                return ctx.NewCallable(pos, "Nothing", {ctx.NewCallable(pos, "PgType", {ctx.NewAtom(pos, pgType->GetName())})});
            } else {
                return ctx.NewCallable(pos, "PgConst", {ctx.NewAtom(pos, PgValueToString(value, pgType->GetId())),
                                                        ctx.NewCallable(pos, "PgType", {ctx.NewAtom(pos, pgType->GetName())})});
            }
        }

        default:
            break;
    }

    YQL_ENSURE(false, "Unsupported type: " << type->GetKind());
}

} // namespace NYql::NCommon
