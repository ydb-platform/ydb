#pragma once
#include "chunks.h"
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TSplittedBlob {
private:
    YDB_READONLY(i64, Size, 0);
    YDB_READONLY_DEF(std::vector<IPortionColumnChunk::TPtr>, Chunks);
public:
    void Take(const IPortionColumnChunk::TPtr& chunk);
    bool operator<(const TSplittedBlob& item) const {
        return Size > item.Size;
    }
};
}
