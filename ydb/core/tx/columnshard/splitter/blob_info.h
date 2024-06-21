#pragma once
#include "chunks.h"
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TSplittedBlob {
private:
    YDB_READONLY_DEF(TString, GroupName);
    YDB_READONLY(i64, Size, 0);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IPortionDataChunk>>, Chunks);

public:
    TSplittedBlob(const TString& groupName)
        : GroupName(groupName)
    {

    }

    void Take(const std::shared_ptr<IPortionDataChunk>& chunk);
    bool operator<(const TSplittedBlob& item) const {
        return Size > item.Size;
    }
};
}
