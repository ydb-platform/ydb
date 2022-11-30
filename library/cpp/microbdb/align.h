#pragma once

#include <util/system/defaults.h>

using TDatAlign = int;

static inline size_t DatFloor(size_t size) {
    return (size - 1) & ~(sizeof(TDatAlign) - 1);
}

static inline size_t DatCeil(size_t size) {
    return DatFloor(size) + sizeof(TDatAlign);
}

static inline void DatSet(void* ptr, size_t size) {
    *(TDatAlign*)((char*)ptr + DatFloor(size)) = 0;
}
