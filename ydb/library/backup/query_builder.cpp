#include "query_builder.h"

#include "backup.h"

#include <ydb/library/dynumber/dynumber.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/builder.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NYdb::NBackup {

static constexpr i64 METERING_ROW_PRECISION = 1024;

////////////////////////////////////////////////////////////////////////////////
// TQueryBuilder
////////////////////////////////////////////////////////////////////////////////

TString TQueryBuilder::BuildQuery(const TString &path) {
    TStringStream query;
    query.Reserve(1024);
    query << "--!syntax_v1\n";
    query << "DECLARE $items AS List<Struct<";

    bool needsComma = false;
    for (auto& col : Columns) {
        if (needsComma) {
            query << ", ";
        }
        query << "'" << col.Name << "'" << ": " << col.Type;
        needsComma = true;
    }
    query << ">>; ";
    query << "REPLACE INTO `" << path << "` "
       << "SELECT * FROM AS_TABLE($items);";

    return query.Str();
}

template<typename T>
T TryParse(const TStringBuf& buf) {
    T tmp;
    TMemoryInput stream(buf);
    stream >> tmp;
    return tmp;
}

template<>
TString TryParse(const TStringBuf& buf) {
    Y_ENSURE(buf.Size() >= 1 && buf.front() == '"' && buf.back() == '"',
            "Source string neither surrounded by quotes nor equals to null, string# " << TString{buf}.Quote());
    TString tmp;
    TMemoryInput stream(buf.Data() + 1, buf.Size() - 2);
    stream >> tmp;
    CGIUnescape(tmp);
    return tmp;
}

template<>
TInstant TryParse(const TStringBuf& buf) {
    return TInstant::ParseIso8601(buf);
}

template<>
bool TryParse(const TStringBuf& buf) {
    return TryParse<ui32>(buf) ? true : false;
}

void TQueryBuilder::AddPrimitiveMember(EPrimitiveType type, TStringBuf buf) {
    switch (type) {

    case EPrimitiveType::Bool:
        Value.Bool(TryParse<bool>(buf));
        break;

    case EPrimitiveType::Int8:
        Value.Int8(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint8:
        Value.Uint8(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int16:
        Value.Int16(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint16:
        Value.Uint16(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int32:
        Value.Int32(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint32:
        Value.Uint32(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int64:
        Value.Int64(TryParse<i64>(buf));
        break;

    case EPrimitiveType::Uint64:
        Value.Uint64(TryParse<ui64>(buf));
        break;

    case EPrimitiveType::Float:
        Value.Float(TryParse<float>(buf));
        break;

    case EPrimitiveType::Double:
        Value.Double(TryParse<double>(buf));
        break;

    case EPrimitiveType::Date:
        Value.Date(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Datetime:
        Value.Datetime(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Timestamp:
        Value.Timestamp(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Interval:
        Value.Interval(TryParse<i64>(buf));
        break;

    case EPrimitiveType::Date32:
        Value.Date32(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Datetime64:
        Value.Datetime64(TryParse<i64>(buf));
        break;        

    case EPrimitiveType::Timestamp64:
        Value.Timestamp64(TryParse<i64>(buf));
        break;        

    case EPrimitiveType::Interval64:
        Value.Interval64(TryParse<i64>(buf));
        break;

    case EPrimitiveType::TzDate:
        Value.TzDate(TryParse<TString>(buf));
        break;

    case EPrimitiveType::TzDatetime:
        Value.TzDatetime(TryParse<TString>(buf));
        break;

    case EPrimitiveType::TzTimestamp:
        Value.TzTimestamp(TryParse<TString>(buf));
        break;

    case EPrimitiveType::String:
        Value.String(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Utf8:
        Value.Utf8(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Yson:
        Value.Yson(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Json:
        Value.Json(TryParse<TString>(buf));
        break;

    case EPrimitiveType::JsonDocument:
        Value.JsonDocument(TryParse<TString>(buf));
        break;

    case EPrimitiveType::DyNumber:
        Y_ENSURE(NKikimr::NDyNumber::IsValidDyNumberString(buf));
        Value.DyNumber(TString(buf));
        break;

    case EPrimitiveType::Uuid:
        Y_ENSURE(false, TStringBuilder() << "Unexpected Primitive kind while parsing line: " << type);
        break;

    }
}

void TQueryBuilder::BuildType(TTypeParser& typeParser, TTypeBuilder& typeBuilder, const TString& name) {
    switch (typeParser.GetKind()) {
    case TTypeParser::ETypeKind::Primitive:
        typeBuilder.Primitive(typeParser.GetPrimitive());
        break;
    case TTypeParser::ETypeKind::Decimal:
        typeBuilder.Decimal(typeParser.GetDecimal());
        break;
    case TTypeParser::ETypeKind::Optional:
        typeParser.OpenOptional();
        typeBuilder.BeginOptional();
        BuildType(typeParser, typeBuilder, name);
        typeBuilder.EndOptional();
        typeParser.CloseOptional();
        break;
    default:
        throw yexception() << "Unsupported type kind \"" << typeParser.GetKind() << "\" for column: " << name;
    }
}

TType TQueryBuilder::GetType(TTypeParser& typeParser, const TString& name) {
    TTypeBuilder typeBuilder;
    BuildType(typeParser, typeBuilder, name);
    return typeBuilder.Build();
}

void TQueryBuilder::CheckNull(const TString& name, TStringBuf buf) {
    if (buf == "null") {
        throw yexception() << "Wrong value \"null\" for non-optional column: " << name;
    }
}

void TQueryBuilder::AddMemberFromString(TTypeParser& type, const TString& name, TStringBuf buf) {
    switch (type.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            CheckNull(name, buf);
            AddPrimitiveMember(type.GetPrimitive(), buf);
            break;
        case TTypeParser::ETypeKind::Optional:
            type.OpenOptional();
            if (buf == "null") {
                Value.EmptyOptional(GetType(type, name));
            } else {
                Value.BeginOptional();
                AddMemberFromString(type, name, buf);
                Value.EndOptional();
            }
            type.CloseOptional();
            break;
        case TTypeParser::ETypeKind::Decimal:
            CheckNull(name, buf);
            Value.Decimal(TDecimalValue(TString(buf), type.GetDecimal().Precision, type.GetDecimal().Scale));
            break;
        default:
            throw yexception() << "Unsupported type kind \"" << type.GetKind() << "\" for column: " << name;
    }
}

void TQueryBuilder::Begin() {
    Value.BeginList();
}

void TQueryBuilder::AddLine(TStringBuf line) {
    Value.AddListItem();
    Value.BeginStruct();
    for (const auto& col : Columns) {
        TStringBuf tok = line.NextTok(',');
        Y_ENSURE(tok, "Empty token on line");
        TTypeParser type(col.Type);
        Value.AddMember(col.Name);
        AddMemberFromString(type, col.Name, tok);
    }
    Value.EndStruct();
}

TValue TQueryBuilder::EndAndGetResultingValue() {
    Value.EndList();
    return Value.Build();
}

TParams TQueryBuilder::EndAndGetResultingParams() {
    // TODO: Use that size in bandwidth limit calculation
    // const auto& valueProto = TProtoAccessor::GetProto(buildedValue);
    // Cout << "Size of valueProto# " << valueProto.ByteSizeLong() << Endl;
    TParamsBuilder paramsBuilder;
    paramsBuilder.AddParam("$items", EndAndGetResultingValue());
    return paramsBuilder.Build();
}

TString TQueryBuilder::GetQueryString() const {
    return Query;
}

////////////////////////////////////////////////////////////////////////////////
// TQueryFromFileIterator
////////////////////////////////////////////////////////////////////////////////

void TQueryFromFileIterator::TryReadNextLines() {
    if (!LinesBunch.Empty() || BytesRemaining == 0) {
        return;
    }

    const auto bytesToRead = Min<i64>(BufferMaxSize, BytesRemaining);
    Y_ENSURE(bytesToRead > 0, "There is no more bytes to read!" <<
             " BufferMaxSize# " << BufferMaxSize <<
             " BytesRemaining# " << BytesRemaining <<
             " CurrentOffset# " << CurrentOffset <<
             " this->Empty()# " << this->Empty());
    IoBuff.resize(bytesToRead);
    i64 bytesRead = DataFile.Pread(IoBuff.Detach(), bytesToRead, CurrentOffset);
    IoBuff.resize(bytesRead);
    size_t newSize = IoBuff.rfind("\n");
    Y_ENSURE(newSize != TString::npos, "Can't find new line symbol in buffer read from file,"
             " bytesRead# " << bytesRead);
    // +1 for newline symbol
    newSize += 1;
    IoBuff.resize(newSize);
    BytesRemaining -= newSize;
    CurrentOffset += newSize;
    LinesBunch = IoBuff;
}

template<bool GetValue>
std::conditional_t<GetValue, TValue, TParams> TQueryFromFileIterator::ReadNext() {
    TryReadNextLines();

    TStringBuf line = LinesBunch.NextTok('\n');
    Query.Begin();
    i64 querySizeRows = 0;
    i64 querySizeBytes = 0;
    while (line || LinesBunch) {
        if (line.empty()) {
            continue;
        }
        Query.AddLine(line);
        ++querySizeRows;
        querySizeBytes += AlignUp<i64>(line.Size(), METERING_ROW_PRECISION);
        if (MaxRowsPerQuery > 0 && querySizeRows >= MaxRowsPerQuery
                || MaxBytesPerQuery > 0 && querySizeBytes >= MaxBytesPerQuery) {
            break;
        }
        line = LinesBunch.NextTok('\n');
    }
    Y_ENSURE(querySizeRows > 0, "No new lines is read from file. Maybe buffer size is less then size of single row");
    if constexpr (GetValue) {
        return Query.EndAndGetResultingValue();
    } else {
        return Query.EndAndGetResultingParams();
    }
}

template std::conditional_t<true, TValue, TParams> TQueryFromFileIterator::ReadNext<true>();
template std::conditional_t<false, TValue, TParams> TQueryFromFileIterator::ReadNext<false>();

TString TQueryFromFileIterator::GetQueryString() const {
    return Query.GetQueryString();
}

} // NYdb::NBackup
