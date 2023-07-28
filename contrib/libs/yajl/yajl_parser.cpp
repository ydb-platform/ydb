#include "yajl_parser.h"

#include <errno.h>

#include <util/string/cast.h>

long long
yajl_parse_integer(const unsigned char *number, unsigned int length) {
    try {
        return FromString<long long>((const char*)number, length);
    } catch (const yexception& ex) {
        errno = ERANGE;
        return (*number == '-') ? LLONG_MIN : LLONG_MAX;
    }
}

unsigned long long
yajl_parse_unsigned_integer(const unsigned char *number, unsigned int length) {
    try {
        return FromString<unsigned long long>((const char*)number, length);
    } catch (const yexception& ex) {
        errno = ERANGE;
        return (*number == '-') ? 0ull : ULLONG_MAX;
    }
}

extern "C" void FormatDoubleYandex(char* buf, size_t len, double val) {
    buf[ToString(val, buf, len)] = 0;
}
