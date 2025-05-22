#include <DataTypes/Serializations/SerializationDate32.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

namespace NDB
{

namespace
{

inline void readText(ExtendedDayNum & date, ReadBuffer & istr, const FormatSettings & settings)
{
    if (!settings.date_format.empty()) {
        readDateTextFormat(date, istr, settings.date_format);
        return;
    }

    readDateText(date, istr);
}

}

void SerializationDate32::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = ExtendedDayNum(assert_cast<const ColumnType &>(column).getData()[row_num]);
    if (!settings.date_format.empty()) {
        writeDateTextFormat(value, ostr, settings.date_format);
        return;
    }

    writeDateText(value, ostr);
}

void SerializationDate32::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void SerializationDate32::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    readText(x, istr, settings);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

void SerializationDate32::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate32::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate32::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    assertChar('\'', istr);
    readText(x, istr, settings);
    assertChar('\'', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDate32::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    assertChar('"', istr);
    readText(x, istr, settings);
    assertChar('"', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

void SerializationDate32::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    
    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, istr, settings);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}
}
