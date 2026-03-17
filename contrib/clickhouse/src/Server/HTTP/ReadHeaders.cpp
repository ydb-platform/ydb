#include <Server/HTTP/ReadHeaders.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <DBPoco/Net/NetException.h>

namespace DB
{

void readHeaders(
    DBPoco::Net::MessageHeader & headers, ReadBuffer & in, size_t max_fields_number, size_t max_name_length, size_t max_value_length)
{
    char ch = 0;  // silence uninitialized warning from gcc-*
    std::string name;
    std::string value;

    name.reserve(32);
    value.reserve(64);

    size_t fields = 0;

    while (true)
    {
        if (fields > max_fields_number)
            throw DBPoco::Net::MessageException("Too many header fields");

        name.clear();
        value.clear();

        /// Field name
        while (in.peek(ch) && ch != ':' && !DBPoco::Ascii::isSpace(ch) && name.size() <= max_name_length)
        {
            name += ch;
            in.ignore();
        }

        if (in.eof())
            throw DBPoco::Net::MessageException("Field is invalid");

        if (name.empty())
        {
            if (ch == '\r')
                /// Start of the empty-line delimiter
                break;
            if (ch == ':')
                throw DBPoco::Net::MessageException("Field name is empty");
        }
        else
        {
            if (name.size() > max_name_length)
                throw DBPoco::Net::MessageException("Field name is too long");
            if (ch != ':')
                throw DBPoco::Net::MessageException(fmt::format("Field name is invalid or no colon found: \"{}\"", name));
        }

        in.ignore();

        skipWhitespaceIfAny(in, true);

        if (in.eof())
            throw DBPoco::Net::MessageException("Field is invalid");

        /// Field value - folded values not supported.
        while (in.read(ch) && ch != '\r' && ch != '\n' && value.size() <= max_value_length)
            value += ch;

        if (in.eof())
            throw DBPoco::Net::MessageException("Field is invalid");

        if (ch == '\n')
            throw DBPoco::Net::MessageException("No CRLF found");

        if (value.size() > max_value_length)
            throw DBPoco::Net::MessageException("Field value is too long");

        skipToNextLineOrEOF(in);

        DBPoco::trimRightInPlace(value);
        headers.add(name, DBPoco::Net::MessageHeader::decodeWord(value));
        ++fields;
    }
}

}
