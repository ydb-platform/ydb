#include "ydb_yson_value.h"

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <util/string/builder.h>

namespace NYdb {

static void PrimitiveValueToYson(EPrimitiveType type, TValueParser& parser, NYson::TYsonWriter& writer)
{
    switch (type) {
        case EPrimitiveType::Bool:
            writer.OnBooleanScalar(parser.GetBool());
            break;
        case EPrimitiveType::Int8:
            writer.OnInt64Scalar(parser.GetInt8());
            break;
        case EPrimitiveType::Uint8:
            writer.OnUint64Scalar(parser.GetUint8());
            break;
        case EPrimitiveType::Int16:
            writer.OnInt64Scalar(parser.GetInt16());
            break;
        case EPrimitiveType::Uint16:
            writer.OnUint64Scalar(parser.GetUint16());
            break;
        case EPrimitiveType::Int32:
            writer.OnInt64Scalar(parser.GetInt32());
            break;
        case EPrimitiveType::Uint32:
            writer.OnUint64Scalar(parser.GetUint32());
            break;
        case EPrimitiveType::Int64:
            writer.OnInt64Scalar(parser.GetInt64());
            break;
        case EPrimitiveType::Uint64:
            writer.OnUint64Scalar(parser.GetUint64());
            break;
        case EPrimitiveType::Float:
            writer.OnDoubleScalar(parser.GetFloat());
            break;
        case EPrimitiveType::Double:
            writer.OnDoubleScalar(parser.GetDouble());
            break;
        case EPrimitiveType::Date:
            writer.OnUint64Scalar(parser.GetDate().Days());
            break;
        case EPrimitiveType::Datetime:
            writer.OnUint64Scalar(parser.GetDatetime().Seconds());
            break;
        case EPrimitiveType::Timestamp:
            writer.OnUint64Scalar(parser.GetTimestamp().MicroSeconds());
            break;
        case EPrimitiveType::Interval:
            writer.OnInt64Scalar(parser.GetInterval());
            break;
        case EPrimitiveType::Date32:
            writer.OnInt64Scalar(parser.GetDate32());
            break;
        case EPrimitiveType::Datetime64:
            writer.OnInt64Scalar(parser.GetDatetime64());
            break;
        case EPrimitiveType::Timestamp64:
            writer.OnInt64Scalar(parser.GetTimestamp64());
            break;
        case EPrimitiveType::Interval64:
            writer.OnInt64Scalar(parser.GetInterval64());
            break;
        case EPrimitiveType::TzDate:
            writer.OnStringScalar(parser.GetTzDate());
            break;
        case EPrimitiveType::TzDatetime:
            writer.OnStringScalar(parser.GetTzDatetime());
            break;
        case EPrimitiveType::TzTimestamp:
            writer.OnStringScalar(parser.GetTzTimestamp());
            break;
        case EPrimitiveType::String:
            writer.OnStringScalar(parser.GetString());
            break;
        case EPrimitiveType::Utf8:
            writer.OnStringScalar(parser.GetUtf8());
            break;
        case EPrimitiveType::Yson:
            writer.OnStringScalar(parser.GetYson());
            break;
        case EPrimitiveType::Json:
            writer.OnStringScalar(parser.GetJson());
            break;
        case EPrimitiveType::JsonDocument:
            writer.OnStringScalar(parser.GetJsonDocument());
            break;
        case EPrimitiveType::Uuid:
            writer.OnStringScalar(parser.GetUuid().ToString());
            break;
        case EPrimitiveType::DyNumber:
            writer.OnStringScalar(parser.GetDyNumber());
            break;
        default:
            ThrowFatalError(TStringBuilder() << "Unsupported primitive type: " << type);
    }
}

static void FormatValueYsonInternal(TValueParser& parser, NYson::TYsonWriter& writer)
{
    switch (parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            PrimitiveValueToYson(parser.GetPrimitiveType(), parser, writer);
            break;

        case TTypeParser::ETypeKind::Decimal:
            writer.OnStringScalar(parser.GetDecimal().ToString());
            break;

        case TTypeParser::ETypeKind::Pg:
            if (parser.GetPg().IsNull()) {
                writer.OnEntity();
            } else if (parser.GetPg().IsText()) {
                writer.OnStringScalar(parser.GetPg().Content_);
            } else {
                writer.OnBeginList();
                writer.OnListItem();
                writer.OnStringScalar(parser.GetPg().Content_);
                writer.OnEndList();
            }
            break;

        case TTypeParser::ETypeKind::Optional:
            parser.OpenOptional();
            if (parser.IsNull()) {
                writer.OnEntity();
            } else {
                writer.OnBeginList();
                writer.OnListItem();
                FormatValueYsonInternal(parser, writer);
                writer.OnEndList();
            }
            parser.CloseOptional();
            break;

        case TTypeParser::ETypeKind::Tagged:
            parser.OpenTagged();
            FormatValueYsonInternal(parser, writer);
            parser.CloseTagged();
            break;

        case TTypeParser::ETypeKind::EmptyList:
            writer.OnBeginList();
            writer.OnEndList();
            break;

        case TTypeParser::ETypeKind::List:
            parser.OpenList();
            writer.OnBeginList();

            while (parser.TryNextListItem()) {
                writer.OnListItem();
                FormatValueYsonInternal(parser, writer);
            }

            writer.OnEndList();
            parser.CloseList();
            break;

        case TTypeParser::ETypeKind::Struct:
            parser.OpenStruct();
            writer.OnBeginList();

            while (parser.TryNextMember()) {
                writer.OnListItem();
                FormatValueYsonInternal(parser, writer);
            }

            writer.OnEndList();
            parser.CloseStruct();
            break;

        case TTypeParser::ETypeKind::Tuple:
            parser.OpenTuple();
            writer.OnBeginList();

            while (parser.TryNextElement()) {
                writer.OnListItem();
                FormatValueYsonInternal(parser, writer);
            }

            writer.OnEndList();
            parser.CloseTuple();
            break;

        case TTypeParser::ETypeKind::EmptyDict:
            writer.OnBeginList();
            writer.OnEndList();
            break;

        case TTypeParser::ETypeKind::Dict:
            parser.OpenDict();
            writer.OnBeginList();
            while (parser.TryNextDictItem()) {
                writer.OnListItem();
                writer.OnBeginList();

                writer.OnListItem();
                parser.DictKey();
                FormatValueYsonInternal(parser, writer);

                writer.OnListItem();
                parser.DictPayload();
                FormatValueYsonInternal(parser, writer);

                writer.OnEndList();
            }
            writer.OnEndList();
            parser.CloseDict();
            break;

        case TTypeParser::ETypeKind::Void:
            writer.OnStringScalar("Void");
            break;

        case TTypeParser::ETypeKind::Null:
            writer.OnEntity();
            break;

        default:
            ThrowFatalError(TStringBuilder() << "Unsupported type kind: " << parser.GetKind());
    }
}

void FormatValueYson(const TValue& value, NYson::TYsonWriter& writer)
{
    TValueParser parser(value);
    FormatValueYsonInternal(parser, writer);
}

TString FormatValueYson(const TValue& value, NYson::EYsonFormat ysonFormat)
{
    TStringStream out;
    NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, true);

    FormatValueYson(value, writer);

    return out.Str();
}

void FormatResultSetYson(const TResultSet& result, NYson::TYsonWriter& writer)
{
    auto columns = result.GetColumnsMeta();

    TResultSetParser parser(result);
    writer.OnBeginList();

    while (parser.TryNextRow()) {
        writer.OnListItem();
        writer.OnBeginList();
        for (ui32 i = 0; i < columns.size(); ++i) {
            writer.OnListItem();
            FormatValueYsonInternal(parser.ColumnParser(i), writer);
        }
        writer.OnEndList();
    }

    writer.OnEndList();
}

TString FormatResultSetYson(const TResultSet& result, NYson::EYsonFormat ysonFormat)
{
    TStringStream out;
    NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, true);

    FormatResultSetYson(result, writer);

    return out.Str();
}

} // namespace NYdb
