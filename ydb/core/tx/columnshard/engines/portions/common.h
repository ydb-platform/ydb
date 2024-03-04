#pragma once
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TChunkAddress {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui16, Chunk, 0);
public:
    TChunkAddress(const ui32 columnId, const ui16 chunk)
        : ColumnId(columnId)
        , Chunk(chunk) {

    }

    bool operator<(const TChunkAddress& address) const {
        return std::tie(ColumnId, Chunk) < std::tie(address.ColumnId, address.Chunk);
    }

    bool operator==(const TChunkAddress& address) const {
        return std::tie(ColumnId, Chunk) == std::tie(address.ColumnId, address.Chunk);
    }

    TString DebugString() const;
};


}
