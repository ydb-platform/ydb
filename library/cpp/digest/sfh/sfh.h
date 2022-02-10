#pragma once

#include <util/system/defaults.h>
#include <util/system/unaligned_mem.h>

inline ui32 SuperFastHash(const void* d, size_t l) noexcept {
    ui32 hash = (ui32)l;
    ui32 tmp;

    if (!l || !d)
        return 0;

    TUnalignedMemoryIterator<ui16, 4> iter(d, l);

    while (!iter.AtEnd()) {
        hash += (ui32)iter.Next();
        tmp = ((ui32)iter.Next() << 11) ^ hash;
        hash = (hash << 16) ^ tmp;
        hash += hash >> 11;
    }

    switch (iter.Left()) {
        case 3:
            hash += (ui32)iter.Next();
            hash ^= hash << 16;
            hash ^= ((ui32)(i32) * (const i8*)iter.Last()) << 18;
            hash += hash >> 11;
            break;

        case 2:
            hash += (ui32)iter.Cur();
            hash ^= hash << 11;
            hash += hash >> 17;
            break;

        case 1:
            hash += *((const i8*)iter.Last());
            hash ^= hash << 10;
            hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}
