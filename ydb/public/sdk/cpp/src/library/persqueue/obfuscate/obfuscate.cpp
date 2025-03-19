#include "obfuscate.h"

#include <util/generic/utility.h>

namespace NPersQueue {
inline namespace Dev {

std::string ObfuscateString(std::string str) {
    ui32 publicPartSize = Min<ui32>(4, str.size() / 4);
    for (ui32 i = publicPartSize; i < str.size() - publicPartSize; ++i) {
        str[i] = '*';
    }
    return str;
}

}
}
