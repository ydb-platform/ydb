#include "utf8.h"

#include <util/charset/wide.h>

#include <ctype.h>
#include <vector>

namespace NYql {

namespace {

unsigned char GetRange(unsigned char c) {
    // Referring to DFA of http://bjoern.hoehrmann.de/utf-8/decoder/dfa/
    // With new mapping 1 -> 0x10, 7 -> 0x20, 9 -> 0x40, such that AND operation can test multiple types.
    static const unsigned char type[] = {
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,0x10,
        0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,
        0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,
        0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,
        8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
        10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8,
    };
    return type[c];
}

struct TByteRange {
    ui8 First = 0;
    ui8 Last = 0;
};

struct TUtf8Ranges {
    size_t BytesCount = 0;
    TByteRange Bytes[4] = {};
};

// see https://lemire.me/blog/2018/05/09/how-quickly-can-you-check-that-a-string-is-valid-unicode-utf-8
inline static const std::vector<TUtf8Ranges> Utf8Ranges = {
    { 1, { {0x00, 0x7f}, {0x00, 0x00}, {0x00, 0x00}, {0x00, 0x00}, } },
    { 2, { {0xc2, 0xdf}, {0x80, 0xbf}, {0x00, 0x00}, {0x00, 0x00}, } },
    { 3, { {0xe0, 0xe0}, {0xa0, 0xbf}, {0x80, 0xbf}, {0x00, 0x00}, } },
    { 3, { {0xe1, 0xec}, {0x80, 0xbf}, {0x80, 0xbf}, {0x00, 0x00}, } },
    { 3, { {0xed, 0xed}, {0x80, 0x9f}, {0x80, 0xbf}, {0x00, 0x00}, } },
    { 3, { {0xee, 0xef}, {0x80, 0xbf}, {0x80, 0xbf}, {0x00, 0x00}, } },
    { 4, { {0xf0, 0xf0}, {0x90, 0xbf}, {0x80, 0xbf}, {0x80, 0xbf}, } },
    { 4, { {0xf1, 0xf3}, {0x80, 0xbf}, {0x80, 0xbf}, {0x80, 0xbf}, } },
    { 4, { {0xf4, 0xf4}, {0x80, 0x8f}, {0x80, 0xbf}, {0x80, 0xbf}, } },
};

std::optional<std::string> RoundBadUtf8(size_t range, std::string_view inputString, size_t pos,
    bool roundDown)
{
    Y_ENSURE(range > 0);
    Y_ENSURE(range < Utf8Ranges.size());

    const std::string prefix{inputString.substr(0, pos)};
    std::string_view suffix = inputString.substr(pos, Utf8Ranges[range].BytesCount);

    std::string newSuffix;
    if (roundDown) {
        std::optional<size_t> lastNonMin;
        for (size_t i = 0; i < suffix.size(); ++i) {
            if (Utf8Ranges[range].Bytes[i].First < ui8(suffix[i])) {
                lastNonMin = i;
            }
        }

        if (!lastNonMin) {
            for (size_t i = 0; i < Utf8Ranges[range - 1].BytesCount; ++i) {
                newSuffix.push_back(Utf8Ranges[range - 1].Bytes[i].Last);
            }
        } else {
            for (size_t i = 0; i < Utf8Ranges[range].BytesCount; ++i) {
                if (i < *lastNonMin) {
                    ui8 c = suffix[i];
                    newSuffix.push_back(c);
                } else if (i == *lastNonMin) {
                    ui8 c = suffix[i];
                    newSuffix.push_back(std::min<ui8>(c - 1, Utf8Ranges[range].Bytes[i].Last));
                } else {
                    newSuffix.push_back(Utf8Ranges[range].Bytes[i].Last);
                }
            }
        }
    } else {
        std::optional<size_t> lastNonMax;
        bool valid = true;
        for (size_t i = 0; i < suffix.size(); ++i) {
            ui8 last = Utf8Ranges[range].Bytes[i].Last;
            ui8 first = Utf8Ranges[range].Bytes[i].First;
            ui8 curr = ui8(suffix[i]);

            valid = valid && curr <= last && curr >= first;
            if (curr < last) {
                lastNonMax = i;
            }
        }

        if (valid) {
            newSuffix = suffix;
            for (size_t i = suffix.size(); i < Utf8Ranges[range].BytesCount; ++i) {
                newSuffix.push_back(Utf8Ranges[range].Bytes[i].First);
            }
        } else if (!lastNonMax) {
            return NextValidUtf8(prefix);
        } else {
            for (size_t i = 0; i < Utf8Ranges[range].BytesCount; ++i) {
                if (i < *lastNonMax) {
                    ui8 c = suffix[i];
                    newSuffix.push_back(c);
                } else if (i == *lastNonMax) {
                    ui8 c = suffix[i];
                    newSuffix.push_back(std::max<ui8>(c + 1, Utf8Ranges[range].Bytes[i].First));
                } else {
                    newSuffix.push_back(Utf8Ranges[range].Bytes[i].First);
                }
            }
        }

    }
    return prefix + newSuffix;
}

}

bool IsUtf8(const std::string_view& str) {
    for (auto it = str.cbegin(); str.cend() != it;) {
#define COPY() if (str.cend() != it) { c = *it++; } else { return false; }
#define TRANS(mask) result &= ((GetRange(static_cast<unsigned char>(c)) & mask) != 0)
#define TAIL() COPY(); TRANS(0x70)
        auto c = *it++;
        if (!(c & 0x80))
            continue;

        bool result = true;
        switch (GetRange(static_cast<unsigned char>(c))) {
        case 2: TAIL(); break;
        case 3: TAIL(); TAIL(); break;
        case 4: COPY(); TRANS(0x50); TAIL(); break;
        case 5: COPY(); TRANS(0x10); TAIL(); TAIL(); break;
        case 6: TAIL(); TAIL(); TAIL(); break;
        case 10: COPY(); TRANS(0x20); TAIL(); break;
        case 11: COPY(); TRANS(0x60); TAIL(); TAIL(); break;
        default: return false;
        }

        if (!result) return false;
#undef COPY
#undef TRANS
#undef TAIL
    }
    return true;
}

unsigned char WideCharSize(char head) {
    switch (GetRange(static_cast<unsigned char>(head))) {
        case 0: return 1;
        case 2: return 2;
        case 3: return 3;
        case 4: return 3;
        case 5: return 4;
        case 6: return 4;
        case 10: return 3;
        case 11: return 4;
        default: return 0;
    }
}

std::optional<std::string> RoundToNearestValidUtf8(const std::string_view& str, bool roundDown) {
    const size_t ss = str.size();
    for (size_t pos = 0; pos < ss; ) {
        ui8 c = str[pos];

        for (size_t i = 0; i < Utf8Ranges.size(); ++i) {
            auto& range = Utf8Ranges[i];

            if (c < range.Bytes[0].First) {
                return RoundBadUtf8(i, str, pos, roundDown);
            }

            if (c <= range.Bytes[0].Last) {
                // valid UTF8 code point start
                for (size_t j = 1; j < range.BytesCount; ++j) {
                    if (pos + j >= ss) {
                        return RoundBadUtf8(i, str, pos, roundDown);
                    }
                    ui8 cur = str[pos + j];
                    if (!(cur >= range.Bytes[j].First && cur <= range.Bytes[j].Last)) {
                        return RoundBadUtf8(i, str, pos, roundDown);
                    }
                }

                pos += range.BytesCount;
                break;
            } else if (i + 1 == Utf8Ranges.size()) {
                if (!roundDown) {
                    return NextValidUtf8(str.substr(0,  pos));
                }
                return RoundBadUtf8(i, str, pos, roundDown);
            }
        }
    }
    return std::string(str);
}

std::optional<std::string> NextValidUtf8(const std::string_view& str) {
    Y_ENSURE(IsUtf8(str));
    TUtf32String wide = UTF8ToUTF32<false>(str);
    bool incremented = false;
    size_t toDrop = 0;
    for (auto it = wide.rbegin(); it != wide.rend(); ++it) {
        auto& c = *it;
        if (c < 0x10ffff) {
            c = (c == 0xd7ff) ? 0xe000 : (c + 1);
            incremented = true;
            break;
        } else {
            ++toDrop;
        }
    }

    if (!incremented) {
        return {};
    }

    Y_ENSURE(toDrop < wide.size());
    wide.resize(wide.size() - toDrop);

    TString result = WideToUTF8(wide);
    return std::string(result.data(), result.size());
}

std::optional<std::string> NextLexicographicString(const std::string_view& str) {
    bool incremented = false;
    size_t toDrop = 0;
    std::string result{str};
    for (auto it = result.rbegin(); it != result.rend(); ++it) {
        auto& c = *it;
        if (ui8(c) < 0xff) {
            ++c;
            incremented = true;
            break;
        } else {
            ++toDrop;
        }
    }

    if (!incremented) {
        return {};
    }

    Y_ENSURE(toDrop < result.size());
    result.resize(result.size() - toDrop);
    return result;
}

}
