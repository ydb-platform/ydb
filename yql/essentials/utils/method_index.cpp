#include "method_index.h"
#include <util/generic/yexception.h>
#include <util/string/hex.h>

namespace NYql {

size_t GetMethodPtrIndex(uintptr_t ptr) {
#ifdef _win_
    size_t offset;
    if (memcmp((void*)ptr, "\x48\x8B\x01\xFF", 4) == 0) {
        if (*(ui8*)(ptr + 4) == 0x60) {
            offset = *(ui8*)(ptr + 5);
        } else if (*(ui8*)(ptr + 4) == 0xa0) {
            offset = *(ui32*)(ptr + 5);
        } else {
            ythrow yexception() << "Unsupported code: " << HexEncode((char*)ptr + 4, 1);
        }
    } else if (memcmp((void*)ptr, "\x50\x48\x89\x0c\x24\x48\x8b\x0c\x24\x48\x8b\x01\x48\x8b", 14) == 0) {
        if (*(ui8*)(ptr + 14) == 0x40) {
            offset = *(ui8*)(ptr + 15);
        } else if (*(ui8*)(ptr + 14) == 0x80) {
            offset = *(ui32*)(ptr + 15);
        } else {
            ythrow yexception() << "Unsupported code: " << HexEncode((char*)ptr + 14, 1);
        }
    } else if (memcmp((void*)ptr, "\x48\x8b\x01\x48\x8b", 5) == 0) {
        if (*(ui8*)(ptr + 5) == 0x40) {
            offset = *(ui8*)(ptr + 6);
        } else if (*(ui8*)(ptr + 5) == 0x80) {
            offset = *(ui32*)(ptr + 6);
        } else {
            ythrow yexception() << "Unsupported code: " << HexEncode((char*)ptr + 5, 1);
        }
    } else {
        ythrow yexception() << "Unsupported code: " << HexEncode((char*)ptr, 16);
    }

    return offset / 8 + 1;
#else
    return ptr >> 3;
#endif
}

}
