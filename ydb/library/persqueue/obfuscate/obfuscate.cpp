#include "obfuscate.h"

#include <util/generic/string.h>

namespace NPersQueue {

TString ObfuscateString(TString str) {
    ui32 publicPartSize = Min<ui32>(4, str.size() / 4);
    for (ui32 i = publicPartSize; i < str.size() - publicPartSize; ++i) {
        str[i] = '*';
    }
    return str;
}

} // namespace NPersQueue
