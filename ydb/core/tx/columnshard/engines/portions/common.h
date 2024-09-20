#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/formats/arrow/save_load/saver.h>

namespace NKikimr::NOlap {
using TColumnSaver = NArrow::NAccessor::TColumnSaver;

class TChunkAddress {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui16, Chunk, 0);
public:
    ui32 GetEntityId() const {
        return ColumnId;
    }

    ui32 GetChunkIdx() const {
        return Chunk;
    }

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

template<>
struct ::THash<NKikimr::NOlap::TChunkAddress> {
    inline ui64 operator()(const NKikimr::NOlap::TChunkAddress& a) const {
        return ((ui64)a.GetEntityId()) << 16 + a.GetChunkIdx();
    }
};
