#include "csv_parser.h"

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/ydb_cli/common/common.h>

#include <library/cpp/string_utils/csv/csv.h>

#include <regex>

namespace NYdb {
namespace NConsoleClient {
namespace {

class TCsvToYdbConverter {
public:
    explicit TCsvToYdbConverter(TTypeParser& parser, const std::optional<TString>& nullValue)
        : Parser(parser)
        , NullValue(nullValue) {}

    template <class T, std::enable_if_t<std::is_integral_v<T> && std::is_signed_v<T>, std::nullptr_t> = nullptr>
    static i64 StringToArithmetic(const TString& token, size_t& cnt) {
        return std::stoll(token, &cnt);
    }

    template <class T, std::enable_if_t<std::is_integral_v<T> && std::is_unsigned_v<T>, std::nullptr_t> = nullptr>
    static ui64 StringToArithmetic(const TString& token, size_t& cnt) {
        return std::stoull(token, &cnt);
    }

    template <class T, std::enable_if_t<std::is_same_v<T, float>, std::nullptr_t> = nullptr>
    static float StringToArithmetic(const TString& token, size_t& cnt) {
        return std::stof(token, &cnt);
    }

    template <class T, std::enable_if_t<std::is_same_v<T, double>, std::nullptr_t> = nullptr>
    static double StringToArithmetic(const TString& token, size_t& cnt) {
        return std::stod(token, &cnt);
    }

    template <class T>
    T GetArithmetic(const TString& token) const {
        size_t cnt;
        try {
            auto value = StringToArithmetic<T>(token, cnt);
            if (cnt != token.size() || value < std::numeric_limits<T>::lowest() || value > std::numeric_limits<T>::max()) {
                throw yexception();
            }
            return static_cast<T>(value);
        } catch (std::exception& e) {
            throw TCsvParseException() << "Expected " << Parser.GetPrimitive() << " value, received: \"" << token << "\".";
        }
    }

    void BuildPrimitive(const TString& token) {
        switch (Parser.GetPrimitive()) {
        case EPrimitiveType::Int8:
            Builder.Int8(GetArithmetic<i8>(token));
            break;
        case EPrimitiveType::Int16:
            Builder.Int16(GetArithmetic<i16>(token));
            break;
        case EPrimitiveType::Int32:
            Builder.Int32(GetArithmetic<i32>(token));
            break;
        case EPrimitiveType::Int64:
            Builder.Int64(GetArithmetic<i64>(token));
            break;
        case EPrimitiveType::Uint8:
            Builder.Uint8(GetArithmetic<ui8>(token));
            break;
        case EPrimitiveType::Uint16:
            Builder.Uint16(GetArithmetic<ui16>(token));
            break;
        case EPrimitiveType::Uint32:
            Builder.Uint32(GetArithmetic<ui32>(token));
            break;
        case EPrimitiveType::Uint64:
            Builder.Uint64(GetArithmetic<ui64>(token));
            break;
        case EPrimitiveType::Bool:
            Builder.Bool(GetBool(token));
            break;
        case EPrimitiveType::String:
            Builder.String(token);
            break;
        case EPrimitiveType::Utf8:
            Builder.Utf8(token);
            break;
        case EPrimitiveType::Json:
            Builder.Json(token);
            break;
        case EPrimitiveType::JsonDocument:
            Builder.JsonDocument(token);
            break;
        case EPrimitiveType::Yson:
            Builder.Yson(token);
            break;
        case EPrimitiveType::Uuid:
            Builder.Uuid(TUuidValue{token});
            break;
        case EPrimitiveType::Float:
            Builder.Float(GetArithmetic<float>(token));
            break;
        case EPrimitiveType::Double:
            Builder.Double(GetArithmetic<double>(token));
            break;
        case EPrimitiveType::DyNumber:
            Builder.DyNumber(token);
            break;
        case EPrimitiveType::Date: {
            TInstant date;
            if (!TInstant::TryParseIso8601(token, date)) {
                date = TInstant::Days(GetArithmetic<ui16>(token));
            }
            Builder.Date(date);
            break;
        }
        case EPrimitiveType::Datetime: {
            TInstant datetime;
            if (!TInstant::TryParseIso8601(token, datetime)) {
                datetime = TInstant::Seconds(GetArithmetic<ui32>(token));
            }
            Builder.Datetime(datetime);
            break;
        }
        case EPrimitiveType::Timestamp: {
            TInstant timestamp;
            if (!TInstant::TryParseIso8601(token, timestamp)) {
                timestamp = TInstant::MicroSeconds(GetArithmetic<ui64>(token));
            }
            Builder.Timestamp(timestamp);
            break;
        }
        case EPrimitiveType::Interval:
            Builder.Interval(GetArithmetic<i64>(token));
            break;
        case EPrimitiveType::Date32: {
            TInstant date;
            if (TInstant::TryParseIso8601(token, date)) {
                Builder.Date32(date.Days());
            } else {
                Builder.Date32(GetArithmetic<i32>(token));
            }
            break;
        }
        case EPrimitiveType::Datetime64: {
            TInstant date;
            if (TInstant::TryParseIso8601(token, date)) {
                Builder.Datetime64(date.Seconds());
            } else {
                Builder.Datetime64(GetArithmetic<i64>(token));
            }
            break;
        }
        case EPrimitiveType::Timestamp64: {
            TInstant date;
            if (TInstant::TryParseIso8601(token, date)) {
                Builder.Timestamp64(date.MicroSeconds());
            } else {
                Builder.Timestamp64(GetArithmetic<i64>(token));
            }
            break;
        }
        case EPrimitiveType::Interval64:
            Builder.Interval64(GetArithmetic<i64>(token));
            break;
        case EPrimitiveType::TzDate:
            Builder.TzDate(token);
            break;
        case EPrimitiveType::TzDatetime:
            Builder.TzDatetime(token);
            break;
        case EPrimitiveType::TzTimestamp:
            Builder.TzTimestamp(token);
            break;
        default:
            throw TCsvParseException() << "Unsupported primitive type: " << Parser.GetPrimitive();
        }
    }

    void BuildValue(const TStringBuf& token) {
        switch (Parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive: {
            BuildPrimitive(std::string{token});
            break;
        }
        case TTypeParser::ETypeKind::Decimal: {
            Builder.Decimal(TDecimalValue(std::string(token), Parser.GetDecimal().Precision, Parser.GetDecimal().Scale));
            break;
        }
        case TTypeParser::ETypeKind::Optional: {
            Parser.OpenOptional();
            if (NullValue && token == NullValue) {
                Builder.EmptyOptional(GetType());
            } else {
                Builder.BeginOptional();
                BuildValue(token);
                Builder.EndOptional();
            }
            Parser.CloseOptional();
            break;
        }
        case TTypeParser::ETypeKind::Null: {
            EnsureNull(token);
            break;
        }
        case TTypeParser::ETypeKind::Void: {
            EnsureNull(token);
            break;
        }
        case TTypeParser::ETypeKind::Tagged: {
            Parser.OpenTagged();
            Builder.BeginTagged(Parser.GetTag());
            BuildValue(token);
            Builder.EndTagged();
            Parser.CloseTagged();
            break;
        }
        case TTypeParser::ETypeKind::Pg: {
            if (NullValue && token == NullValue) {
                Builder.Pg(TPgValue(TPgValue::VK_NULL, {}, Parser.GetPg()));
            } else {
                Builder.Pg(TPgValue(TPgValue::VK_TEXT, TString(token), Parser.GetPg()));
            }
            break;
        }
        default:
            throw TCsvParseException() << "Unsupported type kind: " << Parser.GetKind();
        }
    }

    void BuildType(TTypeBuilder& typeBuilder) {
        switch (Parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            typeBuilder.Primitive(Parser.GetPrimitive());
            break;

        case TTypeParser::ETypeKind::Decimal:
            typeBuilder.Decimal(Parser.GetDecimal());
            break;

        case TTypeParser::ETypeKind::Optional:
            Parser.OpenOptional();
            typeBuilder.BeginOptional();
            BuildType(typeBuilder);
            typeBuilder.EndOptional();
            Parser.CloseOptional();
            break;

        case TTypeParser::ETypeKind::Tagged:
            Parser.OpenTagged();
            typeBuilder.BeginTagged(Parser.GetTag());
            BuildType(typeBuilder);
            typeBuilder.EndTagged();
            Parser.CloseTagged();
            break;

        case TTypeParser::ETypeKind::Pg:
            typeBuilder.Pg(Parser.GetPg());
            break;

        default:
            throw TCsvParseException() << "Unsupported type kind: " << Parser.GetKind();
        }
    }

    TType GetType() {
        TTypeBuilder typeBuilder;
        BuildType(typeBuilder);
        return typeBuilder.Build();
    }

    bool GetBool(const TString& token) const {
        if (token == "true") {
            return true;
        }
        if (token == "false") {
            return false;
        }
        throw TCsvParseException() << "Expected bool value: \"true\" or \"false\", received: \"" << token << "\".";
    }

    void EnsureNull(const TStringBuf& token) const {
        if (!NullValue) {
            throw TCsvParseException() << "Expected null value instead of \"" << token << "\", but null value is not set.";
        }
        if (token != NullValue) {
            throw TCsvParseException() << "Expected null value: \"" << NullValue << "\", received: \"" << token << "\".";
        }
    }

    template <class T>
    bool TryParseArithmetic(const TString& token) const {
        size_t cnt;
        try {
            auto value = StringToArithmetic<T>(token, cnt);
            if (cnt != token.size() || value < std::numeric_limits<T>::lowest() || value > std::numeric_limits<T>::max()) {
                return false;
            }
        } catch (std::exception& e) {
            return false;
        }
        return true;
    }

    bool TryParseBool(const TString& token) const {
        TString tokenLowerCase = to_lower(token);
        return tokenLowerCase == "true" || tokenLowerCase == "false";
    }

    bool TryParsePrimitive(const TString& token) {
        switch (Parser.GetPrimitive()) {
        case EPrimitiveType::Uint8:
            return TryParseArithmetic<ui8>(token) && !token.StartsWith('-');
        case EPrimitiveType::Uint16:
            return TryParseArithmetic<ui16>(token) && !token.StartsWith('-');;
        case EPrimitiveType::Uint32:
            return TryParseArithmetic<ui32>(token) && !token.StartsWith('-');;
        case EPrimitiveType::Uint64:
            return TryParseArithmetic<ui64>(token) && !token.StartsWith('-');;
        case EPrimitiveType::Int8:
            return TryParseArithmetic<i8>(token);
        case EPrimitiveType::Int16:
            return TryParseArithmetic<i16>(token);
        case EPrimitiveType::Int32:
            return TryParseArithmetic<i32>(token);
        case EPrimitiveType::Int64:
            return TryParseArithmetic<i64>(token);
        case EPrimitiveType::Bool:
            return TryParseBool(token);
        case EPrimitiveType::Json:
            return token.StartsWith('{') && token.EndsWith('}');
            break;
        case EPrimitiveType::JsonDocument:
            break;
        case EPrimitiveType::Yson:
            break;
        case EPrimitiveType::Uuid:
            static std::regex uuidRegexTemplate("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
            return std::regex_match(token.c_str(), uuidRegexTemplate);
        case EPrimitiveType::Float:
            return TryParseArithmetic<float>(token);
        case EPrimitiveType::Double:
            return TryParseArithmetic<double>(token);
        case EPrimitiveType::DyNumber:
            break;
        case EPrimitiveType::Date: {
            TInstant date;
            return TInstant::TryParseIso8601(token, date) && token.length() <= 10;
        }
        case EPrimitiveType::Datetime: {
            TInstant datetime;
            return TInstant::TryParseIso8601(token, datetime) && token.length() <= 19;
        }
        case EPrimitiveType::Timestamp: {
            TInstant timestamp;
            return TInstant::TryParseIso8601(token, timestamp) || TryParseArithmetic<ui64>(token);
        }
        case EPrimitiveType::Interval:
            break;
        case EPrimitiveType::Date32: {
            TInstant date;
            return TInstant::TryParseIso8601(token, date) || TryParseArithmetic<i32>(token);
        }
        case EPrimitiveType::Datetime64: {
            TInstant date;
            return TInstant::TryParseIso8601(token, date) || TryParseArithmetic<i64>(token);
        }
        case EPrimitiveType::Timestamp64: {
            TInstant date;
            return TInstant::TryParseIso8601(token, date) || TryParseArithmetic<i64>(token);
        }
        case EPrimitiveType::Interval64:
            return TryParseArithmetic<i64>(token);
        case EPrimitiveType::TzDate:
            break;
        case EPrimitiveType::TzDatetime:
            break;
        case EPrimitiveType::TzTimestamp:
            break;
        default:
            throw TCsvParseException() << "Unsupported primitive type: " << Parser.GetPrimitive();
        }
        return false;
    }

    TValue Convert(const TStringBuf& token) {
        BuildValue(token);
        return Builder.Build();
    }

private:
    TTypeParser& Parser;
    const std::optional<TString> NullValue = "";
    TValueBuilder Builder;
};

TCsvParseException FormatError(const std::exception& inputError,
                               const TCsvParser::TParseMetadata& meta,
                               const std::optional<std::string>& columnName = {}) {
    auto outputError = TCsvParseException() << "Error during CSV parsing";
    if (meta.Line.has_value()) {
        outputError << " in line " << meta.Line.value();
    }
    if (columnName.has_value()) {
        outputError << " in column `" << columnName.value() << "`";
    }
    if (meta.Filename.has_value()) {
        outputError << " in file `" << meta.Filename.value() << "`";
    }
    outputError << ":\n" << inputError.what();
    return outputError;
}

TValue FieldToValue(TTypeParser& parser,
                    const TStringBuf& token,
                    const std::optional<TString>& nullValue,
                    const TCsvParser::TParseMetadata& meta,
                    const std::string& columnName) {
    try {
        TCsvToYdbConverter converter(parser, nullValue);
        return converter.Convert(token);
    } catch (std::exception& e) {
        throw FormatError(e, meta, columnName);
    }
}

TStringBuf Consume(NCsvFormat::CsvSplitter& splitter,
                   const TCsvParser::TParseMetadata& meta,
                   const TString& columnName) {
    try {
        return splitter.Consume();
    } catch (std::exception& e) {
        throw FormatError(e, meta, columnName);
    }
}

}

TCsvParser::TCsvParser(TString&& headerRow, const char delimeter, const std::optional<TString>& nullValue,
                       const std::map<std::string, TType>* destinationTypes,
                       const std::map<TString, TString>* paramSources)
    : HeaderRow(std::move(headerRow))
    , Delimeter(delimeter)
    , NullValue(nullValue)
    , DestinationTypes(destinationTypes)
    , ParamSources(paramSources)
{
    NCsvFormat::CsvSplitter splitter(HeaderRow, Delimeter);
    Header = static_cast<TVector<TString>>(splitter);
}

TCsvParser::TCsvParser(TVector<TString>&& header, const char delimeter, const std::optional<TString>& nullValue,
                       const std::map<std::string, TType>* destinationTypes,
                       const std::map<TString, TString>* paramSources)
    : Header(std::move(header))
    , Delimeter(delimeter)
    , NullValue(nullValue)
    , DestinationTypes(destinationTypes)
    , ParamSources(paramSources)
{
}

void TCsvParser::BuildParams(TString& data, TParamsBuilder& builder, const TParseMetadata& meta) const {
    NCsvFormat::CsvSplitter splitter(data, Delimeter);
    auto headerIt = Header.begin();
    do {
        if (headerIt == Header.end()) {
            throw FormatError(yexception() << "Header contains less fields than data. Header: \"" << HeaderRow << "\", data: \"" << data << "\"", meta);
        }
        TStringBuf token = Consume(splitter, meta, *headerIt);
        TString fullname = "$" + *headerIt;
        auto paramIt = DestinationTypes->find(fullname);
        if (paramIt == DestinationTypes->end()) {
            ++headerIt;
            continue;
        }
        if (ParamSources) {
            auto paramSource = ParamSources->find(fullname);
            if (paramSource != ParamSources->end()) {
                throw FormatError(yexception() << "Parameter " << fullname << " value found in more than one source: stdin, " << paramSource->second << ".", meta);
            }
        }
        TTypeParser parser(paramIt->second);
        builder.AddParam(fullname, FieldToValue(parser, token, NullValue, meta, *headerIt));
        ++headerIt;
    } while (splitter.Step());

    if (headerIt != Header.end()) {
        throw FormatError(yexception() << "Header contains more fields than data. Header: \"" << HeaderRow << "\", data: \"" << data << "\"", meta);
    }
}

void TCsvParser::BuildValue(TString& data, TValueBuilder& builder, const TType& type, const TParseMetadata& meta) const {
    NCsvFormat::CsvSplitter splitter(data, Delimeter);
    auto headerIt = Header.cbegin();
    std::map<std::string, TStringBuf> fields;
    do {
        if (headerIt == Header.cend()) {
            throw FormatError(yexception() << "Header contains less fields than data. Header: \"" << HeaderRow << "\", data: \"" << data << "\"", meta);
        }
        TStringBuf token = Consume(splitter, meta, *headerIt);
        fields[*headerIt] = token;
        ++headerIt;
    } while (splitter.Step());

    if (headerIt != Header.cend()) {
        throw FormatError(yexception() << "Header contains more fields than data. Header: \"" << HeaderRow << "\", data: \"" << data << "\"", meta);
    }

    builder.BeginStruct();
    TTypeParser parser(type);
    parser.OpenStruct();
    while (parser.TryNextMember()) {
        std::string name = parser.GetMemberName();
        if (name == "__ydb_skip_column_name") {
            continue;
        }
        auto fieldIt = fields.find(name);
        if (fieldIt == fields.end()) {
            throw FormatError(yexception() << "No member \"" << name << "\" in csv string for YDB struct type", meta);
        }
        builder.AddMember(name, FieldToValue(parser, fieldIt->second, NullValue, meta, name));
    }

    parser.CloseStruct();
    builder.EndStruct();
}

TValue TCsvParser::BuildList(const std::vector<TString>& lines, const TString& filename, std::optional<ui64> row) const {
    std::vector<std::unique_ptr<TTypeParser>> columnTypeParsers;
    columnTypeParsers.reserve(ResultColumnCount);
    for (const TType* type : ResultLineTypesSorted) {
        columnTypeParsers.push_back(std::make_unique<TTypeParser>(*type));
    }

    // Original path with local value object
    Ydb::Value listProtoValue;
    auto* listItems = listProtoValue.mutable_items();
    listItems->Reserve(lines.size());

    for (const auto& line : lines) {
        ProcessCsvLine(line, listItems, columnTypeParsers, row, filename);
        if (row.has_value()) {
            ++row.value();
        }
    }

    // Return a TValue that takes ownership via move
    return TValue(ResultListType.value(), std::move(listProtoValue));
}

TValue TCsvParser::BuildListOnArena(
    const std::vector<TString>& lines,
    const TString& filename,
    google::protobuf::Arena* arena,
    std::optional<ui64> row
) const {
    Y_ASSERT(arena != nullptr);

    std::vector<std::unique_ptr<TTypeParser>> columnTypeParsers;
    columnTypeParsers.reserve(ResultColumnCount);
    for (const TType* type : ResultLineTypesSorted) {
        columnTypeParsers.push_back(std::make_unique<TTypeParser>(*type));
    }

    // allocate Ydb::Value on arena
    Ydb::Value* listProtoValue = google::protobuf::Arena::CreateMessage<Ydb::Value>(arena);
    auto* listItems = listProtoValue->mutable_items();
    listItems->Reserve(lines.size());

    for (const auto& line : lines) {
        ProcessCsvLine(line, listItems, columnTypeParsers, row, filename);
        if (row.has_value()) {
            ++row.value();
        }
    }

    // Return a TValue that references the arena-allocated message
    return TValue(ResultListType.value(), listProtoValue);
}

// Helper method to process a single CSV line
void TCsvParser::ProcessCsvLine(
    const TString& line,
    google::protobuf::RepeatedPtrField<Ydb::Value>* listItems,
    const std::vector<std::unique_ptr<TTypeParser>>& columnTypeParsers,
    std::optional<ui64> currentRow,
    const TString& filename
) const {
    NCsvFormat::CsvSplitter splitter(line, Delimeter);
    auto* structItems = listItems->Add()->mutable_items();
    structItems->Reserve(ResultColumnCount);

    const TParseMetadata meta {currentRow, filename};

    auto headerIt = Header.cbegin();
    auto skipIt = SkipBitMap.begin();
    auto typeParserIt = columnTypeParsers.begin();

    do {
        if (headerIt == Header.cend()) { // SkipBitMap has same size as Header
            throw FormatError(yexception() << "Header contains less fields than data. Header: \"" << HeaderRow << "\", data: \"" << line << "\"", meta);
        }
        TStringBuf nextField = Consume(splitter, meta, *headerIt);
        if (!*skipIt) {
            TValue builtValue = FieldToValue(*typeParserIt->get(), nextField, NullValue, meta, *headerIt);
            *structItems->Add() = std::move(builtValue.GetProto());

            ++typeParserIt;
        }
        ++headerIt;
        ++skipIt;
    } while (splitter.Step());
}

void TCsvParser::BuildLineType() {
    SkipBitMap.reserve(Header.size());
    ResultColumnCount = 0;
    TTypeBuilder builder;
    builder.BeginStruct();
    for (const auto& colName : Header) {
        auto findIt = DestinationTypes->find(colName);
        if (findIt != DestinationTypes->end()) {
            builder.AddMember(colName, findIt->second);
            ResultLineTypesSorted.push_back(&findIt->second);
            SkipBitMap.push_back(false);
            ++ResultColumnCount;
        } else {
            SkipBitMap.push_back(true);
        }
    }
    builder.EndStruct();
    ResultLineType = builder.Build();
    ResultListType = TTypeBuilder().List(ResultLineType.value()).Build();
}
namespace {
static const std::vector<TType> availableTypes = {
    TTypeBuilder().Primitive(EPrimitiveType::Bool).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Uint64).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Int64).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Double).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Date).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Datetime).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Timestamp).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Json).Build(),
    TTypeBuilder().Primitive(EPrimitiveType::Uuid).Build(),
};

static const auto availableTypesEnd = availableTypes.end();

} // namespace

const TVector<TString>& TCsvParser::GetHeader() {
    return Header;
}

const TString& TCsvParser::GetHeaderRow() const {
    return HeaderRow;
}

bool IsConvertibleToYdbValue(const TString& value, const Ydb::Type& type) {
    try {
        TTypeParser parser(type);
        FieldToValue(parser, value, std::nullopt, {}, "columnName");
        return true;
    } catch (...) {
        return false;
    }
}

}
}

