#include "yql_decimal_serialize.h"

#include <utility>

namespace NYql {
namespace NDecimal {

size_t Serialize(TInt128 value, char* buf) {
    if (value == -Nan()) {
        *buf = 0x00;
        return 1U;
    }

    if (value == -Inf()) {
        *buf = 0x01;
        return 1U;
    }

    if (value == +Inf()) {
        *buf = 0xFE;
        return 1U;
    }

    if (value == +Nan()) {
        *buf = 0xFF;
        return 1U;
    }

    auto size = sizeof(value);
    auto p = reinterpret_cast<const char*>(&value) + size - 1U;

    if (*(p - 1U) & 0x80) {
        while (size > 1U && ~0 == *--p)
            --size;
        *buf = 0x80 - size;
    } else {
        while (size > 1U && 0 == *--p)
            --size;
        *buf = 0x7F + size;
    }

    for (auto i = 1U; i < size; ++i) {
        *++buf = *p--;
    }

    return size;
}

std::pair<TInt128, size_t> Deserialize(const char* b, size_t len) {
    if (!b || len == 0U)
        return std::make_pair(Err(), 0U);

    const auto mark = ui8(*b);
    switch (mark) {
        case 0x00u: return std::make_pair(-Nan(), 1U);
        case 0x01u: return std::make_pair(-Inf(), 1U);
        case 0xFEu: return std::make_pair(+Inf(), 1U);
        case 0xFFu: return std::make_pair(+Nan(), 1U);
    }

    if (mark < 0x70u || mark > 0x8Fu) {
        return std::make_pair(Err(), 0U);
    }

    const bool neg = mark < 0x80u;
    const auto used = neg ? 0x80u - mark : mark - 0x7Fu;
    if (len < used)
        return std::make_pair(Err(), 0U);

    TInt128 v;
    const auto size = sizeof(v);
    auto p = reinterpret_cast<char*>(&v) + size;

    for (auto fill = size - used + 2U; --fill;)
        *--p = neg ? ~0 : 0;

    for (auto copy = used; --copy;)
        *--p = *++b;

    return std::make_pair(v, used);
}

}
}
