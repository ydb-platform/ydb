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
TMaybe<T> TryParse(const TStringBuf& buf) {
    if (buf == "null") {
        return {};
    }

    T tmp;
    TMemoryInput stream(buf);
    stream >> tmp;
    return tmp;
}

template<>
TMaybe<TString> TryParse(const TStringBuf& buf) {
    if (buf == "null") {
        return {};
    }

    Y_ENSURE(buf.Size() >= 1 && buf.front() == '"' && buf.back() == '"',
            "Source string neither surrounded by quotes nor equals to null, string# " << TString{buf}.Quote());
    TString tmp;
    TMemoryInput stream(buf.Data() + 1, buf.Size() - 2);
    stream >> tmp;
    CGIUnescape(tmp);
    return tmp;
}

template<>
TMaybe<TInstant> TryParse(const TStringBuf& buf) {
    if (buf == "null") {
        return {};
    }

    return TInstant::ParseIso8601(buf);
}

template<>
TMaybe<bool> TryParse(const TStringBuf& buf) {
    auto tmp = TryParse<ui32>(buf);
    if (tmp) {
        return *tmp ? true : false;
    } else {
        return {};
    }
}

void TQueryBuilder::AddPrimitiveMember(EPrimitiveType type, TStringBuf buf) {
    switch (type) {

    case EPrimitiveType::Bool:
        Value.OptionalBool(TryParse<bool>(buf));
        break;

    case EPrimitiveType::Int8:
        Value.OptionalInt8(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint8:
        Value.OptionalUint8(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int16:
        Value.OptionalInt16(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint16:
        Value.OptionalUint16(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int32:
        Value.OptionalInt32(TryParse<i32>(buf));
        break;

    case EPrimitiveType::Uint32:
        Value.OptionalUint32(TryParse<ui32>(buf));
        break;

    case EPrimitiveType::Int64:
        Value.OptionalInt64(TryParse<i64>(buf));
        break;

    case EPrimitiveType::Uint64:
        Value.OptionalUint64(TryParse<ui64>(buf));
        break;

    case EPrimitiveType::Float:
        Value.OptionalFloat(TryParse<float>(buf));
        break;

    case EPrimitiveType::Double:
        Value.OptionalDouble(TryParse<double>(buf));
        break;

    case EPrimitiveType::Date:
        Value.OptionalDate(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Datetime:
        Value.OptionalDatetime(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Timestamp:
        Value.OptionalTimestamp(TryParse<TInstant>(buf));
        break;

    case EPrimitiveType::Interval:
        Value.OptionalInterval(TryParse<i64>(buf));
        break;

    case EPrimitiveType::TzDate:
        Value.OptionalTzDate(TryParse<TString>(buf));
        break;

    case EPrimitiveType::TzDatetime:
        Value.OptionalTzDatetime(TryParse<TString>(buf));
        break;

    case EPrimitiveType::TzTimestamp:
        Value.OptionalTzTimestamp(TryParse<TString>(buf));
        break;

    case EPrimitiveType::String:
        Value.OptionalString(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Utf8:
        Value.OptionalUtf8(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Yson:
        Value.OptionalYson(TryParse<TString>(buf));
        break;

    case EPrimitiveType::Json:
        Value.OptionalJson(TryParse<TString>(buf));
        break;

    case EPrimitiveType::JsonDocument:
        Value.OptionalJsonDocument(TryParse<TString>(buf));
        break;

    case EPrimitiveType::DyNumber:
        if (buf == "null") {
            Value.OptionalDyNumber(Nothing());
        } else {
            Y_ENSURE(NKikimr::NDyNumber::IsValidDyNumberString(buf));
            Value.OptionalDyNumber(TString(buf));
        }
        break;

    case EPrimitiveType::Uuid:
        Y_ENSURE(false, TStringBuilder() << "Unexpected Primitive kind while parsing line: " << type);
        break;

    }
}

void TQueryBuilder::AddMemberFromString(const TColumn &col, TStringBuf buf) {
    TTypeParser type(col.Type);
    Y_ENSURE(type.GetKind() == TTypeParser::ETypeKind::Optional);
    type.OpenOptional();

    Value.AddMember(col.Name);
    switch (type.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            AddPrimitiveMember(type.GetPrimitive(), buf);
            break;
        case TTypeParser::ETypeKind::Decimal:
            if (buf == "null") {
                Value.EmptyOptional();
            } else {
                Value.BeginOptional();
                Value.Decimal(TDecimalValue(TString(buf), type.GetDecimal().Precision, type.GetDecimal().Scale));
                Value.EndOptional();
            }
            break;
        default:
            Y_FAIL("");
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
        AddMemberFromString(col, tok);
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
