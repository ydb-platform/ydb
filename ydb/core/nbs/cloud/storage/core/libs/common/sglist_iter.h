#pragma once

#include "public.h"

#include "sglist.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct TSgListIter
{
    const TSgList& SgList;
    size_t Idx = 0;
    size_t Offset = 0;

    TSgListIter(const TSgList& sgList)
        : SgList(sgList)
    {}

    size_t Copy(char* dst, size_t size)
    {
        char* ptr = dst;
        while (Idx < SgList.size() && size) {
            const TBlockDataRef& block = SgList[Idx];
            size_t len = std::min(size, block.Size() - Offset);

            memcpy(ptr, block.Data() + Offset, len);

            ptr += len;
            size -= len;

            Offset += len;
            if (Offset == block.Size()) {
                ++Idx;
                Offset = 0;
            }
        }

        return ptr - dst;
    }
};

}   // namespace NYdb::NBS
