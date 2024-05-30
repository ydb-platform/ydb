#include "ydb_json_value.h"

#include <library/cpp/string_utils/base64/base64.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <library/cpp/json/json_reader.h>
#include <chrono>

namespace NYdb {

    namespace {

    class TUtf8Transcoder
    {
    public:
        explicit TUtf8Transcoder()
        {
        };

        TStringBuf Encode(TStringBuf str) {
            Buffer.clear();
            IsAscii = true;
            for (CurPos = 0; CurPos < str.size(); ++CurPos) {
                ui8 c = str[CurPos];
                if (c == '"' || c == '\\') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back(c);
                } else if (c == '\b') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back('b');
                } else if (c == '\t') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back('t');
                } else if (c == '\f') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back('f');
                } else if (c == '\r') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back('r');
                } else if (c == '\n') {
                    SwitchToNonAscii(str);
                    Buffer.push_back('\\');
                    Buffer.push_back('n');
                } else if (c < '\x20' || c > '\x7E') {
                    SwitchToNonAscii(str);
                    TString tmp = Sprintf("\\u%04X", c);
                    for (unsigned char c : tmp) {
                        Buffer.push_back(c);
                    }
                } else {
                    if (!IsAscii) {
                        Buffer.push_back(c);
                    }
                }
            }
            if (IsAscii) {
                return str;
            } else {
                return TStringBuf(Buffer.data(), Buffer.size());
            }
        }

        TStringBuf Decode(TStringBuf str) {
            Buffer.clear();
            IsAscii = true;
            for (size_t i = 0; i < str.size(); ++i) {
                char c = str[i];
                if (ui8(c) < 128) {
                    if (!IsAscii) {
                        Buffer.push_back(c);
                    }
                } else if ((c & '\xFC') == '\xC0') {
                    if (IsAscii) {
                        Buffer.resize(i);
                        std::copy(str.data(), str.data() + i, Buffer.data());
                        IsAscii = false;
                    }
                    Buffer.push_back(((c & '\x03') << 6) | (str[i + 1] & '\x3F'));
                    i += 1;
                } else {
                    ThrowFatalError("Unicode symbols with codes greater than 255 are not supported.");
                }
            }
            if (IsAscii) {
                return str;
            } else {
                return TStringBuf(Buffer.data(), Buffer.size());
            }
        }

    private:
        void SwitchToNonAscii(TStringBuf& str) {
            if (IsAscii) {
                Buffer.resize(CurPos);
                std::copy(str.data(), str.data() + CurPos, Buffer.data());
                IsAscii = false;
            }
        }

    private:
        bool IsAscii;
        std::vector<char> Buffer;
        size_t CurPos;
    };

    ui32 ParseNumber(ui32& pos, const::std::string_view& buf, ui32& value, i8 dig_cnt) {
        ui32 count = 0U;
        for (value = 0U; dig_cnt && pos < buf.size(); --dig_cnt, ++pos) {
            if (const auto c = buf[pos]; c >= '0' && c <= '9') {
                value = value * 10U + (c - '0');
                ++count;
                continue;
            }
            break;
        }

        return count;
    }

    std::chrono::year_month_day ParseDate(ui32& pos, const std::string_view& buf) {
        bool beforeChrist = false;
        if (pos < buf.size()) {
            switch (buf.data()[pos]) {
                case '-':
                    beforeChrist = true;
                    [[fallthrough]];
                case '+':
                    ++pos;
                    [[fallthrough]];
                default:
                    break;
            }
        }

        ui32 year, month, day;
        if (!ParseNumber(pos, buf, year, 6U) || pos == buf.size() || buf[pos] != '-' ||
            !ParseNumber(++pos, buf, month, 2U) || pos == buf.size() || buf[pos] != '-' ||
            !ParseNumber(++pos, buf, day, 2U)) {
            return {};
        }

        const i32 iyear = beforeChrist ? -year : year;
        return std::chrono::year_month_day{std::chrono::year{iyear}, std::chrono::month{month}, std::chrono::day{day}};
    }

    std::optional<ui32> ParseTime(ui32& pos, const std::string_view& buf) {
        ui32 hour, minute, second;
        if (pos == buf.size() || buf[pos] != 'T' ||
            !ParseNumber(++pos, buf, hour, 2U) || pos == buf.size() || buf[pos] != ':' ||
            !ParseNumber(++pos, buf, minute, 2U) || pos == buf.size() || buf[pos] != ':' ||
            !ParseNumber(++pos, buf, second, 2U) || pos == buf.size() ||
            hour >= 24U || minute >= 60U || second >= 60U) {
            return std::nullopt;
        }

        return hour * 3600U + minute * 60U + second;
    }

    constexpr i64 SecondsInDay = 86400LL;
    constexpr i64 MicroMiltiplier = 1000000LL;

    std::optional<i64> ParseDateTime(ui32& pos, const std::string_view& buf) {
        const auto date = ParseDate(pos, buf);
        if (!date.ok())
            return std::nullopt;

        const auto time = ParseTime(pos, buf);
        if (!time || buf[pos] != 'Z')
            return std::nullopt;

        return i64(std::chrono::sys_days(date).time_since_epoch().count()) * SecondsInDay + i64(*time);
    }

    std::optional<ui32> ParseMicroseconds(ui32& pos, const std::string_view& buf) {
        if (buf[pos] == '.') {
            ui32 ms = 0U;
            auto prevPos = ++pos;
            if (!ParseNumber(pos, buf, ms, 6U)) {
                return std::nullopt;
            }

            for (prevPos = pos - prevPos; prevPos < 6U; ++prevPos) {
                ms *= 10U;
            }

            // Skip unused digits
            while (pos < buf.size() && '0' <= buf[pos] && buf[pos] <= '9') {
                ++pos;
            }
            return ms;
        }
        return 0U;
    }

    std::optional<i64> ParseTimestamp(const std::string_view& buf) {
        ui32 pos = 0U;
        const auto date = ParseDate(pos, buf);
        if (!date.ok())
            return std::nullopt;

        const auto time = ParseTime(pos, buf);
        if (!time)
            return std::nullopt;

        const auto microseconds = ParseMicroseconds(pos, buf);
        if (!microseconds || buf[pos] != 'Z')
            return std::nullopt;

        return (i64(std::chrono::sys_days(date).time_since_epoch().count()) * SecondsInDay + i64(*time)) * MicroMiltiplier + *microseconds;
    }

    void WriteDate(TStringBuilder& out, const i32 days) {
        const std::chrono::year_month_day ymd{std::chrono::sys_days(std::chrono::days(days))};
        if (!ymd.ok()) {
            ThrowFatalError(TStringBuilder() << "Invalid value for date: " << days);
        }
        out << int(ymd.year()) << '-' << LeftPad(unsigned(ymd.month()), 2U, '0') << '-' << LeftPad(unsigned(ymd.day()), 2U, '0');
    }

    void WriteTime(TStringBuilder& out, const ui32 time) {
        out << LeftPad(time / 3600, 2U, '0') << ':' << LeftPad(time % 3600 / 60, 2U, '0') << ':' << LeftPad(time % 60, 2U, '0');
    }

    void WriteDatetime(TStringBuilder& out, i64 datetime) {
        auto date = datetime / SecondsInDay;
        datetime -= date * SecondsInDay;
        if (datetime < 0) {
            --date;
            datetime += SecondsInDay;
        }

        WriteDate(out, i32(date));
        out << 'T';
        WriteTime(out, ui32(datetime));
    }

    TString FormatDate(i32 days) {
        TStringBuilder str;
        WriteDate(str, days);
        return str;
    }

    TString FormatDatetime(i64 datetime) {
        TStringBuilder str;
        WriteDatetime(str, datetime);
        str << 'Z';
        return str;
    }

    TString FormatTimestamp(i64 timestamp) {
        auto datetime = timestamp / MicroMiltiplier;
        timestamp -= datetime * MicroMiltiplier;
        if (timestamp < 0) {
            --datetime;
            timestamp += MicroMiltiplier;
        }
        TStringBuilder str;
        WriteDatetime(str, datetime);
        if (timestamp) {
            str << '.' << LeftPad(timestamp, 6U, '0');
        }
        str << 'Z';
        return str;
    }

    class TYdbToJsonConverter {
    public:
        TYdbToJsonConverter(TValueParser& parser, NJsonWriter::TBuf& writer, EBinaryStringEncoding encoding)
            : Parser(parser)
            , Writer(writer)
            , Encoding(encoding)
        {
        }

        void Convert() {
            ParseValue();
        }

    private:
        void ParsePrimitiveValue(EPrimitiveType type) {
            switch (type) {
            case EPrimitiveType::Bool:
                Writer.WriteBool(Parser.GetBool());
                break;
            case EPrimitiveType::Int8:
                Writer.WriteInt(Parser.GetInt8());
                break;
            case EPrimitiveType::Uint8:
                Writer.WriteInt(Parser.GetUint8());
                break;
            case EPrimitiveType::Int16:
                Writer.WriteInt(Parser.GetInt16());
                break;
            case EPrimitiveType::Uint16:
                Writer.WriteInt(Parser.GetUint16());
                break;
            case EPrimitiveType::Int32:
                Writer.WriteInt(Parser.GetInt32());
                break;
            case EPrimitiveType::Uint32:
                Writer.WriteULongLong(Parser.GetUint32());
                break;
            case EPrimitiveType::Int64:
                Writer.WriteLongLong(Parser.GetInt64());
                break;
            case EPrimitiveType::Uint64:
                Writer.WriteULongLong(Parser.GetUint64());
                break;
            case EPrimitiveType::Float:
                Writer.WriteFloat(Parser.GetFloat(), PREC_AUTO);
                break;
            case EPrimitiveType::Double:
                Writer.WriteDouble(Parser.GetDouble(), PREC_AUTO);
                break;
            case EPrimitiveType::Date:
                Writer.WriteString(Parser.GetDate().FormatGmTime("%Y-%m-%d"));
                break;
            case EPrimitiveType::Datetime:
                Writer.WriteString(Parser.GetDatetime().ToStringUpToSeconds());
                break;
            case EPrimitiveType::Timestamp:
                Writer.WriteString(Parser.GetTimestamp().ToString());
                break;
            case EPrimitiveType::Interval:
                Writer.WriteLongLong(Parser.GetInterval());
                break;
            case EPrimitiveType::Date32:
                Writer.WriteString(FormatDate(Parser.GetDate32()));
                break;
            case EPrimitiveType::Datetime64:
                Writer.WriteString(FormatDatetime(Parser.GetDatetime64()));
                break;
            case EPrimitiveType::Timestamp64:
                Writer.WriteString(FormatTimestamp(Parser.GetTimestamp64()));
                break;
            case EPrimitiveType::Interval64:
                Writer.WriteLongLong(Parser.GetInterval64());
                break;
            case EPrimitiveType::TzDate:
                Writer.WriteString(Parser.GetTzDate());
                break;
            case EPrimitiveType::TzDatetime:
                Writer.WriteString(Parser.GetTzDatetime());
                break;
            case EPrimitiveType::TzTimestamp:
                Writer.WriteString(Parser.GetTzTimestamp());
                break;
            case EPrimitiveType::String:
                Writer.UnsafeWriteValue(BinaryStringToJsonString(Parser.GetString()));
                break;
            case EPrimitiveType::Utf8:
                Writer.WriteString(Parser.GetUtf8());
                break;
            case EPrimitiveType::Yson:
                Writer.UnsafeWriteValue(BinaryStringToJsonString(Parser.GetYson()));
                break;
            case EPrimitiveType::Json:
                Writer.WriteString(Parser.GetJson());
                break;
            case EPrimitiveType::Uuid:
                Writer.WriteString(Parser.GetUuid().ToString());
                break;
            case EPrimitiveType::JsonDocument:
                Writer.WriteString(Parser.GetJsonDocument());
                break;
            case EPrimitiveType::DyNumber:
                Writer.WriteString(Parser.GetDyNumber());
                break;
            default:
                ThrowFatalError(TStringBuilder() << "Unsupported primitive type: " << type);
            }
        }

        void ParseValue() {
            switch (Parser.GetKind()) {
            case TTypeParser::ETypeKind::Primitive:
                ParsePrimitiveValue(Parser.GetPrimitiveType());
                break;

            case TTypeParser::ETypeKind::Decimal:
                Writer.WriteString(Parser.GetDecimal().ToString());
                break;

            case TTypeParser::ETypeKind::Pg:
                if (Parser.GetPg().IsNull()) {
                    Writer.WriteNull();
                } else if (Parser.GetPg().IsText()) {
                    Writer.WriteString(Parser.GetPg().Content_);
                } else {
                    Writer.BeginList();
                    Writer.UnsafeWriteValue(BinaryStringToJsonString(Parser.GetPg().Content_));
                    Writer.EndList();
                }
                break;

            case TTypeParser::ETypeKind::Optional:
                Parser.OpenOptional();
                if (Parser.IsNull()) {
                    Writer.WriteNull();
                } else {
                    ParseValue();
                }
                Parser.CloseOptional();
                break;

            case TTypeParser::ETypeKind::Tagged:
                Parser.OpenTagged();
                ParseValue();
                Parser.CloseTagged();
                break;

            case TTypeParser::ETypeKind::EmptyList:
            {
                Writer.BeginList();
                Writer.EndList();
                break;
            }

            case TTypeParser::ETypeKind::List:
                Parser.OpenList();
                Writer.BeginList();

                while (Parser.TryNextListItem()) {
                    ParseValue();
                }

                Parser.CloseList();
                Writer.EndList();
                break;

            case TTypeParser::ETypeKind::Struct:
                Parser.OpenStruct();
                Writer.BeginObject();

                while (Parser.TryNextMember()) {
                    Writer.WriteKey(Parser.GetMemberName());
                    ParseValue();
                }

                Parser.CloseStruct();
                Writer.EndObject();
                break;

            case TTypeParser::ETypeKind::Tuple:
                Parser.OpenTuple();
                Writer.BeginList();

                while (Parser.TryNextElement()) {
                    ParseValue();
                }

                Parser.CloseTuple();
                Writer.EndList();
                break;

            case TTypeParser::ETypeKind::EmptyDict:
            {
                Writer.BeginList();
                Writer.EndList();
                break;
            }

            case TTypeParser::ETypeKind::Dict:
                Parser.OpenDict();
                Writer.BeginList();

                while (Parser.TryNextDictItem()) {
                    Writer.BeginList();
                    Parser.DictKey();
                    ParseValue();
                    Parser.DictPayload();
                    ParseValue();
                    Writer.EndList();
                }
                Parser.CloseDict();
                Writer.EndList();
                break;
            case TTypeParser::ETypeKind::Null:
                Writer.WriteNull();
                break;
            default:
                ThrowFatalError(TStringBuilder() << "Unsupported type kind: " << Parser.GetKind());
            }
        }

        TString BinaryStringToJsonString(const TString& s) {
            TStringStream str;
            str << "\"";
            switch (Encoding) {
            case EBinaryStringEncoding::Unicode:
                str << Utf8Transcoder.Encode(s);
                break;
            case EBinaryStringEncoding::Base64:
                str << Base64Encode(s);
                break;
            default:
                ThrowFatalError(TStringBuilder() << "Unknown binary string encode mode: "
                    << static_cast<size_t>(Encoding));
            }
            str << "\"";
            return str.Str();
        }

    private:
        TValueParser& Parser;
        NJsonWriter::TBuf& Writer;
        EBinaryStringEncoding Encoding;
        TUtf8Transcoder Utf8Transcoder;
    };
}

void FormatValueJson(const TValue& value, NJsonWriter::TBuf& writer,
    EBinaryStringEncoding encoding)
{
    TValueParser typeParser(value);
    TYdbToJsonConverter converter(typeParser, writer, encoding);
    converter.Convert();
}

TString FormatValueJson(const TValue& value, EBinaryStringEncoding encoding)
{
    TStringStream out;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &out);

    FormatValueJson(value, writer, encoding);

    return out.Str();
}

void FormatResultRowJson(TResultSetParser& parser, const TVector<TColumn>& columns, NJsonWriter::TBuf& writer,
    EBinaryStringEncoding encoding)
{
    writer.BeginObject();
    for (ui32 i = 0; i < columns.size(); ++i) {
        writer.WriteKey(columns[i].Name);
        TYdbToJsonConverter converter(parser.ColumnParser(i), writer, encoding);
        converter.Convert();
    }
    writer.EndObject();
}

TString FormatResultRowJson(TResultSetParser& parser, const TVector<TColumn>& columns,
    EBinaryStringEncoding encoding)
{
    TStringStream out;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &out);

    FormatResultRowJson(parser, columns, writer, encoding);

    return out.Str();
}

void FormatResultSetJson(const TResultSet& result, IOutputStream* out, EBinaryStringEncoding encoding)
{
    auto columns = result.GetColumnsMeta();

    TResultSetParser parser(result);

    while (parser.TryNextRow()) {
        NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, out);
        FormatResultRowJson(parser, columns, writer, encoding);
        *out << Endl;
    }
}

TString FormatResultSetJson(const TResultSet& result, EBinaryStringEncoding encoding)
{
    TStringStream out;

    FormatResultSetJson(result, &out, encoding);

    return out.Str();
}

namespace {
    class TJsonToYdbConverter {
    public:
        TJsonToYdbConverter(TValueBuilder& valueBuilder, const NJson::TJsonValue& jsonValue, TTypeParser& typeParser,
                EBinaryStringEncoding encoding)
            : ValueBuilder(valueBuilder)
            , JsonValue(jsonValue)
            , TypeParser(typeParser)
            , Encoding(encoding)
        {
        }

        void Convert() {
            ParseValue(JsonValue);
        }

    private:
        void ParsePrimitiveValue(const NJson::TJsonValue& jsonValue, EPrimitiveType type) {
            switch (type) {
            case EPrimitiveType::Bool:
                EnsureType(jsonValue, NJson::JSON_BOOLEAN);
                ValueBuilder.Bool(jsonValue.GetBoolean());
                break;
            case EPrimitiveType::Int8:
            {
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                long long intValue = jsonValue.GetInteger();
                if (intValue > std::numeric_limits<i8>::max() || intValue < std::numeric_limits<i8>::min()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in Int8 type");
                }
                ValueBuilder.Int8(intValue);
                break;
            }
            case EPrimitiveType::Uint8:
            {
                EnsureType(jsonValue, NJson::JSON_UINTEGER);
                unsigned long long intValue = jsonValue.GetUInteger();
                if (intValue > std::numeric_limits<ui8>::max()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in UInt8 type");
                }
                ValueBuilder.Uint8(intValue);
                break;
            }
            case EPrimitiveType::Int16:
            {
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                long long intValue = jsonValue.GetInteger();
                if (intValue > std::numeric_limits<i16>::max() || intValue < std::numeric_limits<i16>::min()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in Int16 type");
                }
                ValueBuilder.Int16(intValue);
                break;
            }
            case EPrimitiveType::Uint16:
            {
                EnsureType(jsonValue, NJson::JSON_UINTEGER);
                unsigned long long intValue = jsonValue.GetUInteger();
                if (intValue > std::numeric_limits<ui16>::max()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in UInt16 type");
                }
                ValueBuilder.Uint16(intValue);
                break;
            }
            case EPrimitiveType::Int32:
            {
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                long long intValue = jsonValue.GetInteger();
                if (intValue > std::numeric_limits<i32>::max() || intValue < std::numeric_limits<i32>::min()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in Int32 type");
                }
                ValueBuilder.Int32(intValue);
                break;
            }
            case EPrimitiveType::Uint32:
            {
                EnsureType(jsonValue, NJson::JSON_UINTEGER);
                unsigned long long intValue = jsonValue.GetUInteger();
                if (intValue > std::numeric_limits<ui32>::max()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in UInt32 type");
                }
                ValueBuilder.Uint32(intValue);
                break;
            }
            case EPrimitiveType::Int64:
            {
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                long long intValue = jsonValue.GetInteger();
                if (intValue > std::numeric_limits<i64>::max() || intValue < std::numeric_limits<i64>::min()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in Int64 type");
                }
                ValueBuilder.Int64(intValue);
                break;
            }
            case EPrimitiveType::Uint64:
            {
                EnsureType(jsonValue, NJson::JSON_UINTEGER);
                unsigned long long intValue = jsonValue.GetUInteger();
                if (intValue > std::numeric_limits<ui64>::max()) {
                    ThrowFatalError(TStringBuilder() << "Value \"" << intValue << "\" doesn't fit in UInt64 type");
                }
                ValueBuilder.Uint64(intValue);
                break;
            }
            case EPrimitiveType::Float:
                EnsureType(jsonValue, NJson::JSON_DOUBLE);
                ValueBuilder.Float(jsonValue.GetDouble());
                break;
            case EPrimitiveType::Double:
                EnsureType(jsonValue, NJson::JSON_DOUBLE);
                ValueBuilder.Double(jsonValue.GetDouble());
                break;
            case EPrimitiveType::Date:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                TInstant date;
                if (!TInstant::TryParseIso8601(jsonValue.GetString(), date)) {
                    ThrowFatalError(TStringBuilder() << "Can't parse date from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Date(date);
                break;
            }
            case EPrimitiveType::Datetime:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                TInstant dateTime;
                if (!TInstant::TryParseIso8601(jsonValue.GetString(), dateTime)) {
                    ThrowFatalError(TStringBuilder() << "Can't parse dateTime from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Datetime(dateTime);
                break;
            }
            case EPrimitiveType::Timestamp:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                TInstant timestamp;
                if (!TInstant::TryParseIso8601(jsonValue.GetString(), timestamp)) {
                    ThrowFatalError(TStringBuilder() << "Can't parse timestamp from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Timestamp(timestamp);
                break;
            }
            case EPrimitiveType::Interval:
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                ValueBuilder.Interval(jsonValue.GetInteger());
                break;
            case EPrimitiveType::Date32:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                ui32 pos = 0U;
                const auto date = ParseDate(pos, jsonValue.GetString());
                if (!date.ok()) {
                    ThrowFatalError(TStringBuilder() << "Can't parse date from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Date32(std::chrono::sys_days(date).time_since_epoch().count());
                break;
            }
            case EPrimitiveType::Datetime64:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                ui32 pos = 0U;
                const auto datetime = ParseDateTime(pos, jsonValue.GetString());
                if (!datetime) {
                    ThrowFatalError(TStringBuilder() << "Can't parse time point from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Datetime64(*datetime);
                break;
            }
            case EPrimitiveType::Timestamp64:
            {
                EnsureType(jsonValue, NJson::JSON_STRING);
                const auto timestamp = ParseTimestamp(jsonValue.GetString());
                if (!timestamp) {
                    ThrowFatalError(TStringBuilder() << "Can't parse timestamp from string \"" << jsonValue.GetString() << "\"");
                }
                ValueBuilder.Timestamp64(*timestamp);
                break;
            }
            case EPrimitiveType::Interval64:
                EnsureType(jsonValue, NJson::JSON_INTEGER);
                ValueBuilder.Interval64(jsonValue.GetInteger());
                break;
            case EPrimitiveType::TzDate:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.TzDate(jsonValue.GetString());
                break;
            case EPrimitiveType::TzDatetime:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.TzDatetime(jsonValue.GetString());
                break;
            case EPrimitiveType::TzTimestamp:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.TzTimestamp(jsonValue.GetString());
                break;
            case EPrimitiveType::String:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.String(JsonStringToBinaryString(jsonValue.GetString()));
                break;
            case EPrimitiveType::Utf8:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.Utf8(jsonValue.GetString());
                break;
            case EPrimitiveType::Yson:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.Yson(JsonStringToBinaryString(jsonValue.GetString()));
                break;
            case EPrimitiveType::Json:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.Json(jsonValue.GetString());
                break;
            case EPrimitiveType::Uuid:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.Uuid(jsonValue.GetString());
                break;
            case EPrimitiveType::JsonDocument:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.JsonDocument(jsonValue.GetString());
                break;
            case EPrimitiveType::DyNumber:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.DyNumber(jsonValue.GetString());
                break;
            default:
                ThrowFatalError(TStringBuilder() << "Unsupported primitive type: " << type);
            }
        }

        void BuildTypeInner(TTypeBuilder& typeBuilder) {
            switch (TypeParser.GetKind()) {
            case TTypeParser::ETypeKind::Primitive:
                typeBuilder.Primitive(TypeParser.GetPrimitive());
                break;
            case TTypeParser::ETypeKind::Decimal:
                typeBuilder.Decimal(TypeParser.GetDecimal());
                break;
            case TTypeParser::ETypeKind::Pg:
                typeBuilder.Pg(TypeParser.GetPg());
                break;
            case TTypeParser::ETypeKind::Optional:
                TypeParser.OpenOptional();
                typeBuilder.BeginOptional();
                BuildTypeInner(typeBuilder);
                typeBuilder.EndOptional();
                TypeParser.CloseOptional();
                break;
            case TTypeParser::ETypeKind::List:
                TypeParser.OpenList();
                typeBuilder.BeginList();
                BuildTypeInner(typeBuilder);
                TypeParser.CloseList();
                typeBuilder.EndList();
                break;
            case TTypeParser::ETypeKind::Struct:
                TypeParser.OpenStruct();
                typeBuilder.BeginStruct();
                while (TypeParser.TryNextMember()) {
                    typeBuilder.AddMember(TypeParser.GetMemberName());
                    BuildTypeInner(typeBuilder);
                }
                TypeParser.CloseStruct();
                typeBuilder.EndStruct();
                break;
            case TTypeParser::ETypeKind::Tuple:
                TypeParser.OpenTuple();
                typeBuilder.BeginTuple();
                while (TypeParser.TryNextMember()) {
                    typeBuilder.AddElement();
                    BuildTypeInner(typeBuilder);
                }
                TypeParser.CloseTuple();
                typeBuilder.EndTuple();
                break;
            case TTypeParser::ETypeKind::Dict:
                TypeParser.OpenDict();
                typeBuilder.BeginDict();
                TypeParser.DictKey();
                typeBuilder.DictKey();
                BuildTypeInner(typeBuilder);
                TypeParser.DictPayload();
                typeBuilder.DictPayload();
                BuildTypeInner(typeBuilder);
                TypeParser.CloseDict();
                typeBuilder.EndDict();
                break;
            default:
                ThrowFatalError(TStringBuilder() << "Unsupported type kind: " << TypeParser.GetKind());
            }
        }

        TType GetType() {
            TTypeBuilder typeBuilder;
            BuildTypeInner(typeBuilder);
            return typeBuilder.Build();
        }

        void ParseValue(const NJson::TJsonValue& jsonValue) {
            switch (TypeParser.GetKind()) {
            case TTypeParser::ETypeKind::Null:
                EnsureType(jsonValue, NJson::JSON_NULL);
                break;

            case TTypeParser::ETypeKind::Primitive:
                ParsePrimitiveValue(jsonValue, TypeParser.GetPrimitive());
                break;

            case TTypeParser::ETypeKind::Decimal:
                EnsureType(jsonValue, NJson::JSON_STRING);
                ValueBuilder.Decimal(jsonValue.GetString());
                break;

            case TTypeParser::ETypeKind::Pg:
                if (jsonValue.GetType() == NJson::JSON_STRING) {
                    ValueBuilder.Pg(TPgValue(TPgValue::VK_TEXT, jsonValue.GetString(), TypeParser.GetPg()));
                } else if (jsonValue.GetType() == NJson::JSON_NULL) {
                    ValueBuilder.Pg(TPgValue(TPgValue::VK_NULL, {}, TypeParser.GetPg()));
                } else {
                    EnsureType(jsonValue, NJson::JSON_ARRAY);
                    if (jsonValue.GetArray().size() != 1) {
                        ThrowFatalError(TStringBuilder() << "Pg type should be encoded as array with size 1, but not " << jsonValue.GetArray().size());
                    }
                    auto& innerJsonValue = jsonValue.GetArray().at(0);
                    EnsureType(innerJsonValue, NJson::JSON_STRING);
                    auto binary = JsonStringToBinaryString(innerJsonValue.GetString());
                    ValueBuilder.Pg(TPgValue(TPgValue::VK_BINARY, binary, TypeParser.GetPg()));
                }
                break;

            case TTypeParser::ETypeKind::Optional:
                TypeParser.OpenOptional();
                if (jsonValue.IsNull() && TypeParser.GetKind() != TTypeParser::ETypeKind::Optional) {
                    ValueBuilder.EmptyOptional(GetType());
                } else {
                    ValueBuilder.BeginOptional();
                    ParseValue(jsonValue);
                    ValueBuilder.EndOptional();
                }
                TypeParser.CloseOptional();
                break;

            case TTypeParser::ETypeKind::Tagged:
                TypeParser.OpenTagged();
                ValueBuilder.BeginTagged(TypeParser.GetTag());
                ParseValue(jsonValue);
                ValueBuilder.EndTagged();
                TypeParser.CloseTagged();
                break;

            case TTypeParser::ETypeKind::EmptyList:
                EnsureType(jsonValue, NJson::JSON_ARRAY);
                break;

            case TTypeParser::ETypeKind::List:
                EnsureType(jsonValue, NJson::JSON_ARRAY);
                TypeParser.OpenList();
                if (jsonValue.GetArray().empty()) {
                    ValueBuilder.EmptyList(GetType());
                } else {
                    ValueBuilder.BeginList();
                    for (const auto& element : jsonValue.GetArray()) {
                        ValueBuilder.AddListItem();
                        ParseValue(element);
                    }
                    ValueBuilder.EndList();
                }
                TypeParser.CloseList();
                break;

            case TTypeParser::ETypeKind::Struct:
            {
                EnsureType(jsonValue, NJson::JSON_MAP);
                TypeParser.OpenStruct();
                ValueBuilder.BeginStruct();

                const auto& jsonMap = jsonValue.GetMap();
                while (TypeParser.TryNextMember()) {
                    const TString& memberName = TypeParser.GetMemberName();
                    const auto it = jsonMap.find(memberName);
                    if (it == jsonMap.end()) {
                        ThrowFatalError(TStringBuilder() << "No member \"" << memberName
                            << "\" in the map in json string for YDB struct type");
                    }
                    ValueBuilder.AddMember(memberName);
                    ParseValue(it->second);
                }

                ValueBuilder.EndStruct();
                TypeParser.CloseStruct();
                break;
            }

            case TTypeParser::ETypeKind::Tuple:
                EnsureType(jsonValue, NJson::JSON_ARRAY);
                TypeParser.OpenTuple();
                ValueBuilder.BeginTuple();

                for (const auto& element : jsonValue.GetArray()) {
                    if (!TypeParser.TryNextElement()) {
                        ThrowFatalError("Tuple in json string should contain less elements than provided");
                    }
                    ValueBuilder.AddElement();
                    ParseValue(element);
                }
                if (TypeParser.TryNextElement()) {
                    ThrowFatalError("Tuple in json string should contain more elements than provided");
                }

                ValueBuilder.EndTuple();
                TypeParser.CloseTuple();
                break;

            case TTypeParser::ETypeKind::EmptyDict:
                EnsureType(jsonValue, NJson::JSON_ARRAY);
                break;

            case TTypeParser::ETypeKind::Dict:
                EnsureType(jsonValue, NJson::JSON_ARRAY);
                TypeParser.OpenDict();

                if (jsonValue.GetArray().size()) {
                    ValueBuilder.BeginDict();
                    for (const auto& keyValueElement : jsonValue.GetArray()) {
                        EnsureType(keyValueElement, NJson::JSON_ARRAY);
                        const auto& keyValueArray = keyValueElement.GetArray();
                        if (keyValueArray.size() != 2) {
                            ThrowFatalError("Each element of a dict type in YDB must be represented with "
                                "exactly 2 elements in array in json string");
                        }
                        auto it = keyValueArray.begin();

                        ValueBuilder.AddDictItem();

                        TypeParser.DictKey();
                        ValueBuilder.DictKey();
                        ParseValue(*it);

                        TypeParser.DictPayload();
                        ValueBuilder.DictPayload();
                        ParseValue(*(++it));
                    }
                    ValueBuilder.EndDict();
                } else {
                    TypeParser.DictKey();
                    TType keyType = GetType();
                    TypeParser.DictPayload();
                    TType payloadType = GetType();
                    ValueBuilder.EmptyDict(keyType, payloadType);
                }

                TypeParser.CloseDict();
                break;

            default:
                ThrowFatalError(TStringBuilder() << "Unsupported type kind: " << TypeParser.GetKind());
            }

        }

        TString JsonStringToBinaryString(const TString& s) {
            TStringStream str;
            switch (Encoding) {
            case EBinaryStringEncoding::Unicode:
                str << Utf8Transcoder.Decode(s);
                break;
            case EBinaryStringEncoding::Base64:
                str << Base64Decode(s);
                break;
            default:
                ThrowFatalError("Unknown binary string encode mode");
                break;
            }
            return str.Str();
        }

        void EnsureType(const NJson::TJsonValue& value, NJson::EJsonValueType type) {
            if (value.GetType() != type) {
                if (value.GetType() == NJson::EJsonValueType::JSON_INTEGER && type == NJson::EJsonValueType::JSON_UINTEGER
                    || value.GetType() == NJson::EJsonValueType::JSON_UINTEGER && type == NJson::EJsonValueType::JSON_INTEGER) {
                    return;
                }
                if ((value.GetType() == NJson::EJsonValueType::JSON_INTEGER || value.GetType() == NJson::EJsonValueType::JSON_UINTEGER)
                    && type == NJson::EJsonValueType::JSON_DOUBLE) {
                    return;
                }
                TStringStream str;
                NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &str);
                writer.WriteJsonValue(&value);
                ThrowFatalError(TStringBuilder() << "Wrong type for json value \"" << str.Str()
                    << "\". Expected type: " << type << ", received type: " << value.GetType() << ". ");
            }
        }

    private:
        TValueBuilder& ValueBuilder;
        const NJson::TJsonValue& JsonValue;
        TTypeParser& TypeParser;
        EBinaryStringEncoding Encoding;
        TUtf8Transcoder Utf8Transcoder;
    };
}

TValue JsonToYdbValue(const TString& jsonString, const TType& type, EBinaryStringEncoding encoding) {
    NJson::TJsonValue jsonValue;

    try {
        if (!NJson::ReadJsonTree(jsonString, &jsonValue, true)) {
            ThrowFatalError(TStringBuilder() << "Can't parse string \"" << jsonString << "\" as json.");
        }
    }
    catch (std::exception& e) {
        ThrowFatalError(
            TStringBuilder() << "Exception while parsing string \"" << jsonString << "\" as json: " << e.what());
    }
    return JsonToYdbValue(jsonValue, type, encoding);
}

TValue JsonToYdbValue(const NJson::TJsonValue& jsonValue, const TType& type, EBinaryStringEncoding encoding) {
    TValueBuilder builder;
    TTypeParser typeParser(type);

    TJsonToYdbConverter converter(builder, jsonValue, typeParser, encoding);
    converter.Convert();

    return builder.Build();
}

} // namespace NYdb
