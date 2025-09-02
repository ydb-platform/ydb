#include "utf8.h"

#include <util/charset/wide.h>

namespace NYdb {
inline namespace Dev {
namespace NIssue {

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

}
}
}
