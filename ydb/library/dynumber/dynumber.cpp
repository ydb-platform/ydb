#include "dynumber.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/stream/buffer.h>
#include <util/stream/format.h>

namespace NKikimr::NDyNumber {

bool IsValidDyNumber(TStringBuf buffer) {
    const auto size = buffer.Size();
    if (!size)
        return false;
    switch (const auto data = buffer.Data(); *data) {
        case '\x00':
            if (size < 2U || size > 21U)
                return false;
            for (auto i = 2U; i < size; ++i)
                if ((data[i] & '\x0F') < '\x06' || ((data[i] >> '\x04') & '\x0F') < '\x06')
                    return false;
            break;
        case '\x01':
            return 1U == size;
        case '\x02':
            if (size < 2U || size > 21U)
                return false;
            for (auto i = 2U; i < size; ++i)
                if ((data[i] & '\x0F') > '\x09' || ((data[i] >> '\x04') & '\x0F') > '\x09')
                    return false;
            break;
        default:
            return false;
    }
    return true;
}

bool IsValidDyNumberString(TStringBuf str) {
    if (str.empty())
        return false;
    auto s = str.data();
    auto l = str.size();
    const bool neg = '-' == *s;
    if (neg || '+' == *s) {
        ++s;
        --l;
    }
    if (!l)
        return false;
    bool hasDot = false;
    auto beforeDot = 0U;
    auto nonZeroAfterDot = 0U;
    bool hasNonZeroAfterDot = false;
    auto zeroAfterDot = 0U;
    i16 ePower = 0;
    auto tailZeros = 0U;
    for (auto i = 0U; i < l; ++i) {
        const auto c = s[i];
        const bool isZero = '0' == c;
        if (!hasDot && isZero && !beforeDot)
            continue;
        if (c == '.') {
            if (hasDot)
                return false;
            hasDot = true;
            continue;
        }
        if (c =='e' || c == 'E') {
            if (++i >= l)
                return false;
            if (!TryFromString(s + i, l - i, ePower))
                return false;
            break;
        }
        if (!std::isdigit(c))
            return false;
        if (!hasDot) {
            ++beforeDot;
        } else {
            if (!isZero)
                hasNonZeroAfterDot = true;
            if (hasNonZeroAfterDot) {
                if (isZero) {
                    ++tailZeros;
                } else {
                    nonZeroAfterDot += tailZeros;
                    tailZeros = 0U;
                }
            } else {
                ++zeroAfterDot;
                if (beforeDot)
                    ++tailZeros;
            }
        }
    }
    auto effectivePower = ePower;
    if (beforeDot)
        effectivePower += beforeDot;
    else if (hasNonZeroAfterDot)
        effectivePower -= zeroAfterDot;
    else
        return true;
    if (beforeDot + zeroAfterDot + nonZeroAfterDot > 38U)
        return false;
    if (effectivePower < -129 || effectivePower > 126)
        return false;
    return true;
}

TMaybe<TString> ParseDyNumberString(TStringBuf str) {
    if (str.empty())
        return Nothing();
    auto s = str.data();
    auto l = str.size();
    const bool neg = '-' == *s;
    if (neg || '+' == *s) {
        ++s;
        --l;
    }
    if (!l)
        return Nothing();
    bool hasDot = false;
    auto beforeDot = 0U;
    auto nonZeroAfterDot = 0U;
    bool hasNonZeroAfterDot = false;
    auto zeroAfterDot = 0U;
    auto tailZerosBeforeDot = 0U;
    i16 ePower = 0;
    auto tailZeros = 0U;
    TSmallVec<char> data;
    data.reserve(l);
    for (auto i = 0U; i < l; ++i) {
        const auto c = s[i];
        const bool isZero = '0' == c;
        if (!hasDot && isZero && !beforeDot)
            continue;
        if (c == '.') {
            if (hasDot)
                return Nothing();
            hasDot = true;
            continue;
        }
        if (c =='e' || c == 'E') {
            if (++i >= l)
                return Nothing();
            if (!TryFromString(s + i, l - i, ePower))
                return Nothing();
            break;
        }
        if (!std::isdigit(c))
            return Nothing();
        if (!hasDot) {
            ++beforeDot;
            if (isZero) {
                ++tailZerosBeforeDot;
            } else {
                for (; tailZerosBeforeDot; --tailZerosBeforeDot) {
                    data.emplace_back('\x00');
                }
                data.emplace_back(c - '0');
            }
        } else {
            if (!isZero)
                hasNonZeroAfterDot = true;
            if (hasNonZeroAfterDot) {
                if (isZero) {
                    ++tailZeros;
                } else {
                    for (; tailZerosBeforeDot; --tailZerosBeforeDot) {
                        data.emplace_back('\x00');
                    }
                    for (; tailZeros; --tailZeros) {
                        data.emplace_back('\x00');
                        ++nonZeroAfterDot;
                    }
                    data.emplace_back(c - '0');
                }
            } else {
                ++zeroAfterDot;
                if (beforeDot)
                    ++tailZeros;
            }
        }
    }
    auto effectivePower = ePower;
    if (beforeDot)
        effectivePower += beforeDot;
    else if (hasNonZeroAfterDot)
        effectivePower -= zeroAfterDot;
    else
        return "\x01";
    if (beforeDot + zeroAfterDot + nonZeroAfterDot > 38U)
        return Nothing();
    if (effectivePower < -129 || effectivePower > 126)
        return Nothing();
    if (data.size() % 2U)
        data.emplace_back('\x00');
    
    TString result;
    result.reserve(2U + (data.size() >> 1U));
    if (neg) {
        result.append('\x00');
        result.append(char(126 - effectivePower));
        for (auto i = 0U; i < data.size(); i += 2U)
            result.append((('\x0F' - data[i])  << '\x04') | ('\x0F' - data[i + 1]));
    } else {
        result.append('\x02');
        result.append(char(effectivePower + 129));
        for (auto i = 0U; i < data.size(); i += 2U)
            result.append((data[i]  << '\x04') | data[i + 1]);
    }

    // Cerr << str << ": " << HexText(TStringBuf{result.c_str(), result.size()}) << Endl;

    return result;
}

TMaybe<TString> DyNumberToString(TStringBuf buffer) {
    TStringBuilder out;
    auto s = buffer.data();
    auto l = buffer.size();
    if (l <= 0U || *s >= '\x03') {
        return Nothing();
    }
    if ('\x01' == *s) {
        if (1U != l) {
            return Nothing();
        }
        out << '0';
        return out;
    }
    const bool negative = !*s++;
    if (negative)
        out << '-';
    if (0U >= --l) {
        return Nothing();
    }
    auto power = ui8(*s++);
    if (negative)
        power = '\xFF' - power;
    out << '.';
    const auto digits = negative ? "FEDCBA9876543210" : "0123456789ABCDEF";
    while (--l) {
        const auto c = *s++;
        out << digits[(c >> '\x04') & '\x0F'];
        if (const auto digit = c & '\x0F'; digit != (negative ? '\x0F' : '\x00') || l > 1U)
            out << digits[digit];
    }
    if (const auto e = power - 129)
        out << 'e' << e;
    return out;
}

}
