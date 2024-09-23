#include "local_pgwire_util.h"
#include "log_impl.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NLocalPgWire {

TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool:
            return TStringBuilder() << valueParser.GetBool();
        case NYdb::EPrimitiveType::Int8:
            return TStringBuilder() << valueParser.GetInt8();
        case NYdb::EPrimitiveType::Uint8:
            return TStringBuilder() << valueParser.GetUint8();
        case NYdb::EPrimitiveType::Int16:
            return TStringBuilder() << valueParser.GetInt16();
        case NYdb::EPrimitiveType::Uint16:
            return TStringBuilder() << valueParser.GetUint16();
        case NYdb::EPrimitiveType::Int32:
            return TStringBuilder() << valueParser.GetInt32();
        case NYdb::EPrimitiveType::Uint32:
            return TStringBuilder() << valueParser.GetUint32();
        case NYdb::EPrimitiveType::Int64:
            return TStringBuilder() << valueParser.GetInt64();
        case NYdb::EPrimitiveType::Uint64:
            return TStringBuilder() << valueParser.GetUint64();
        case NYdb::EPrimitiveType::Float:
            return TStringBuilder() << valueParser.GetFloat();
        case NYdb::EPrimitiveType::Double:
            return TStringBuilder() << valueParser.GetDouble();
        case NYdb::EPrimitiveType::Utf8:
            return TStringBuilder() << valueParser.GetUtf8();
        case NYdb::EPrimitiveType::Date:
            return valueParser.GetDate().ToString();
        case NYdb::EPrimitiveType::Datetime:
            return valueParser.GetDatetime().ToString();
        case NYdb::EPrimitiveType::Timestamp:
            return valueParser.GetTimestamp().ToString();
        case NYdb::EPrimitiveType::Interval:
            return TStringBuilder() << valueParser.GetInterval();
        case NYdb::EPrimitiveType::Date32:
            return TStringBuilder() << valueParser.GetDate32();
        case NYdb::EPrimitiveType::Datetime64:
            return TStringBuilder() << valueParser.GetDatetime64();
        case NYdb::EPrimitiveType::Timestamp64:
            return TStringBuilder() << valueParser.GetTimestamp64();
        case NYdb::EPrimitiveType::Interval64:
            return TStringBuilder() << valueParser.GetInterval64();
        case NYdb::EPrimitiveType::TzDate:
            return valueParser.GetTzDate();
        case NYdb::EPrimitiveType::TzDatetime:
            return valueParser.GetTzDatetime();
        case NYdb::EPrimitiveType::TzTimestamp:
            return valueParser.GetTzTimestamp();
        case NYdb::EPrimitiveType::String:
            return Base64Encode(valueParser.GetString());
        case NYdb::EPrimitiveType::Yson:
            return valueParser.GetYson();
        case NYdb::EPrimitiveType::Json:
            return valueParser.GetJson();
        case NYdb::EPrimitiveType::JsonDocument:
            return valueParser.GetJsonDocument();
        case NYdb::EPrimitiveType::DyNumber:
            return valueParser.GetDyNumber();
        case NYdb::EPrimitiveType::Uuid:
            return valueParser.GetUuid().ToString();
    }
    return {};
}

TString ColumnValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Primitive:
        return ColumnPrimitiveValueToString(valueParser);
    case NYdb::TTypeParser::ETypeKind::Optional: {
        TString value;
        valueParser.OpenOptional();
        if (valueParser.IsNull()) {
            value = "NULL";
        } else {
            value = ColumnValueToString(valueParser);
        }
        valueParser.CloseOptional();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Tuple: {
        TString value;
        valueParser.OpenTuple();
        while (valueParser.TryNextElement()) {
            if (!value.empty()) {
                value += ',';
            }
            value += ColumnValueToString(valueParser);
        }
        valueParser.CloseTuple();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Pg: {
        return valueParser.GetPg().Content_;
    }
    default:
        return {};
    }
}

NPG::TEvPGEvents::TRowValueField ColumnValueToRowValueField(NYdb::TValueParser& valueParser, int16_t format) {
    switch (valueParser.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Primitive:
        return {.Value = ColumnPrimitiveValueToString(valueParser)};
    case NYdb::TTypeParser::ETypeKind::Optional: {
        NPG::TEvPGEvents::TRowValueField value;
        valueParser.OpenOptional();
        if (!valueParser.IsNull()) {
            value = ColumnValueToRowValueField(valueParser, format);
        }
        valueParser.CloseOptional();
        return value;
    }
    case NYdb::TTypeParser::ETypeKind::Tuple: {
        TString value;
        valueParser.OpenTuple();
        while (valueParser.TryNextElement()) {
            if (!value.empty()) {
                value += ',';
            }
            value += ColumnValueToString(valueParser);
        }
        valueParser.CloseTuple();
        return {.Value = value};
    }
    case NYdb::TTypeParser::ETypeKind::Pg: {
        auto pg = valueParser.GetPg();
        if (!pg.IsNull()) {
            switch (format) {
                case EFormatText:
                    return {.Value = pg.Content_};
                case EFormatBinary: {
                    // HACK(xenoxeno)
                    NKikimr::NPg::TConvertResult result = NKikimr::NPg::PgNativeBinaryFromNativeText(pg.Content_, pg.PgType_.Oid);
                    if (result.Error.Empty()) {
                        auto begin(reinterpret_cast<const uint8_t*>(result.Str.data()));
                        auto end(begin + result.Str.size());
                        return {.Value = std::vector<uint8_t>(begin, end)};
                    } else {
                        BLOG_ERROR("Error converting value to binary format: " << result.Error.GetRef());
                    }
                    return {};
                }
            }
        } else {
            return {};
        }
    }
    default:
        return {};
    }
}

uint32_t GetPgOidFromYdbType(NYdb::TType type) {
    NYdb::TTypeParser parser(type);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Pg: {
            return parser.GetPg().Oid;
        default:
            return {};
        }
    }
}

std::optional<NYdb::TPgType> GetPgTypeFromYdbType(NYdb::TType type) {
    NYdb::TTypeParser parser(type);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Pg: {
            return parser.GetPg();
        default:
            return {};
        }
    }
}

Ydb::TypedValue GetTypedValueFromParam(int16_t format, const std::vector<uint8_t>& value, const Ydb::Type& type) {
    Ydb::TypedValue typedValue;
    typedValue.mutable_type()->CopyFrom(type);
    NYdb::TTypeParser parser(type);
    if (parser.GetKind() == NYdb::TTypeParser::ETypeKind::Pg) {
        typedValue.mutable_type()->CopyFrom(type);
        if (format == EFormatText) {
            typedValue.mutable_value()->set_text_value(TString(reinterpret_cast<const char*>(&value.front()), value.size()));
        } else if (format == EFormatBinary) {
            typedValue.mutable_value()->set_bytes_value(TString(reinterpret_cast<const char*>(&value.front()), value.size()));
        } else {
            Y_ABORT_UNLESS(false/*unknown format type*/);
        }
    } else {
        // it's not supported yet
        Y_ABORT_UNLESS(false/*non-PG type*/);
    }
    return typedValue;
}

NYdb::NScripting::TExecuteYqlResult ConvertProtoResponseToSdkResult(Ydb::Scripting::ExecuteYqlResponse&& proto) {
    TVector<NYdb::TResultSet> res;
    TMaybe<NYdb::NTable::TQueryStats> queryStats;
    {
        Ydb::Scripting::ExecuteYqlResult result;
        proto.mutable_operation()->mutable_result()->UnpackTo(&result);
        for (int i = 0; i < result.result_sets_size(); i++) {
            res.emplace_back(std::move(*result.mutable_result_sets(i)));
        }
        if (result.has_query_stats()) {
            queryStats = NYdb::NTable::TQueryStats(std::move(*result.mutable_query_stats()));
        }
    }
    NYdb::TPlainStatus alwaysSuccess;
    return {NYdb::TStatus(std::move(alwaysSuccess)), std::move(res), queryStats};
}

int16_t GetFormatForColumn(size_t index, const std::vector<int16_t>& format) {
    if (format.empty()) {
        return EFormatText;
    }
    if (index < format.size()) {
        return static_cast<EFormatType>(format[index]);
    }
    if (format.size() == 1) {
        return static_cast<EFormatType>(format[0]);
    }
    return EFormatText;
}

void FillResultSet(const NYdb::TResultSet& resultSet, std::vector<NPG::TEvPGEvents::TDataRow>& dataRows, const std::vector<int16_t>& format) {
    NYdb::TResultSetParser parser(std::move(resultSet));
    while (parser.TryNextRow()) {
        dataRows.emplace_back();
        auto& row = dataRows.back();
        row.resize(parser.ColumnsCount());
        for (size_t index = 0; index < parser.ColumnsCount(); ++index) {
            int16_t formatType = GetFormatForColumn(index, format);
            row[index] = ColumnValueToRowValueField(parser.ColumnParser(index), formatType);
        }
    }
}

bool IsQueryEmptyChar(char c) {
    return c == ' ' || c == ';' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

bool IsQueryEmpty(TStringBuf query) {
    for (char c : query) {
        if (!IsQueryEmptyChar(c)) {
            return false;
        }
    }
    return true;
}

}
