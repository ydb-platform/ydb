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
    TSplittedBlob(const TString& groupName, std::vector<std::shared_ptr<IPortionDataChunk>>&& chunks)
        : GroupName(groupName) {
        const auto pred = [](const std::shared_ptr<IPortionDataChunk>& l, const std::shared_ptr<IPortionDataChunk>& r) {
            return l->GetChunkAddressVerified() < r->GetChunkAddressVerified();
        };
        std::sort(chunks.begin(), chunks.end(), pred);
        Chunks = std::move(chunks);
        for (auto&& i : Chunks) {
            Size += i->GetPackedSize();
        }
    }

    TSplittedBlob(const TString& groupName)
        : GroupName(groupName) {
    }

};
}
