#include <cmath>

#include <Common/formatReadable.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace NDB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    }

// I wanted to make this ALWAYS_INLINE to prevent flappy performance tests,
// but GCC complains it may not be inlined.
static void formatReadable(double size, NDB::WriteBuffer & out,
    int precision, const char ** units, size_t units_size, double delimiter)
{
    size_t i = 0;
    for (; i + 1 < units_size && fabs(size) >= delimiter; ++i)
        size /= delimiter;

    NDB::DoubleConverter<false>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = NDB::DoubleConverter<false>::instance().ToFixed(size, precision, &builder);

    if (!result)
        throw NDB::Exception("Cannot print float or double number", NDB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    out.write(buffer, builder.position());
    writeCString(units[i], out);
}


void formatReadableSizeWithBinarySuffix(double value, NDB::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB", " ZiB", " YiB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1024);
}

std::string formatReadableSizeWithBinarySuffix(double value, int precision)
{
    NDB::WriteBufferFromOwnString out;
    formatReadableSizeWithBinarySuffix(value, out, precision);
    return out.str();
}


void formatReadableSizeWithDecimalSuffix(double value, NDB::WriteBuffer & out, int precision)
{
    const char * units[] = {" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableSizeWithDecimalSuffix(double value, int precision)
{
    NDB::WriteBufferFromOwnString out;
    formatReadableSizeWithDecimalSuffix(value, out, precision);
    return out.str();
}


void formatReadableQuantity(double value, NDB::WriteBuffer & out, int precision)
{
    const char * units[] = {"", " thousand", " million", " billion", " trillion", " quadrillion"};
    formatReadable(value, out, precision, units, sizeof(units) / sizeof(units[0]), 1000);
}

std::string formatReadableQuantity(double value, int precision)
{
    NDB::WriteBufferFromOwnString out;
    formatReadableQuantity(value, out, precision);
    return out.str();
}

}
