#include "helpers.h"

#include "escape.h"
#include "format.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TFormatsConsumerBase::TFormatsConsumerBase()
    : Parser(this)
{ }

void TFormatsConsumerBase::OnRaw(TStringBuf yson, EYsonType type)
{
    Parser.Parse(yson, type);
}

void TFormatsConsumerBase::Flush()
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
void WriteInt(T value, IOutputStream* output)
{
    char buf[64];
    char* end = buf + 64;
    char* start = WriteDecIntToBufferBackwards(end, value);
    output->Write(start, end - start);
}

void WriteDouble(double value, IOutputStream* output)
{
    char buf[64];
    char* begin = buf;
    auto length = FloatToString(value, buf, sizeof(buf));
    if (std::find(begin, begin + length, '.') == begin + length &&
        std::find(begin, begin + length, 'e') == begin + length)
    {
        begin[length++] = '.';
    }
    output->Write(begin, length);
}

void WriteUnversionedValue(const TUnversionedValue& value, IOutputStream* output, const TEscapeTable& escapeTable)
{
    switch (value.Type) {
        case EValueType::Null:
            return;
        case EValueType::Int64:
            WriteInt(value.Data.Int64, output);
            return;
        case EValueType::Uint64:
            WriteInt(value.Data.Uint64, output);
            return;
        case EValueType::Double:
            WriteDouble(value.Data.Double, output);
            return;
        case EValueType::Boolean:
            output->Write(FormatBool(value.Data.Boolean));
            return;
        case EValueType::String:
            EscapeAndWrite(value.AsStringBuf(), output, escapeTable);
            return;

        case EValueType::Any:
        case EValueType::Composite:

        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Values of type %Qlv are not supported by the chosen format", value.Type)
        << TErrorAttribute("value", ToString(value));
}

bool IsTrivialIntermediateSchema(const NTableClient::TTableSchema& schema)
{
    // Here we make expected objects.
    // It might be not that efficient, but we don't want this code to be fast, we want it to be reliable.
    // Creating expected objects checks that all attributes have their default values
    // (including future attributes that might be introduced)

    TKeyColumns columnNames;
    for (const auto& column : schema.Columns()) {
        columnNames.push_back(column.Name());
    }

    // Columns are ok, we check other schema attributes.
    auto expectedSchema = TTableSchema::FromKeyColumns(columnNames);
    return schema == *expectedSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
