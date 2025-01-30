#include <DataTypes/Serializations/SerializationDate.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>

#include <Common/assert_cast.h>

namespace NDB
{

namespace
{

inline void readText(DayNum & date, ReadBuffer & istr, const FormatSettings & settings)
{
    if (!settings.date_format.empty()) {
        readDateTextFormat(date, istr, settings.date_format);
        return;
    }

    readDateText(date, istr);
}

}

void SerializationDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = DayNum(assert_cast<const ColumnType &>(column).getData()[row_num]);
    if (!settings.date_format.empty()) {
        writeDateTextFormat(value, ostr, settings.date_format);
        return;
    }

    writeDateText(value, ostr);
}

void SerializationDate::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void SerializationDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    readText(x, istr, settings);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void SerializationDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    assertChar('\'', istr);
    readText(x, istr, settings);
    assertChar('\'', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    assertChar('"', istr);
    readText(x, istr, settings);
    assertChar('"', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void SerializationDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    
    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, istr, settings);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

}
