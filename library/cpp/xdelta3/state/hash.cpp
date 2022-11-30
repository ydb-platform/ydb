#include <util/digest/murmur.h>

namespace NXdeltaAggregateColumn {

    ui32 CalcHash(const ui8* data, size_t size)
    {
        return MurmurHash<ui32>(data, size);
    }

}
