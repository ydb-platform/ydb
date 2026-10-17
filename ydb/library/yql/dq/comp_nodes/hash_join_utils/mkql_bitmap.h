#pragma once

#include "better_mkql_ensure.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

// a copy of the TDynBitMap interface from
// https://github.com/ydb-platform/ydb/blob/main/util/generic/bitmap.h
// that stores its chunks in TMKQLVector and therefore accounts all dynamic storage
// through the MiniKQL allocator
class TMKQLBitMap {
  public:
    void Reserve(size_t bits) {
        Chunks_.resize((bits + BitsPerChunk - 1) / BitsPerChunk, 0);
    }

    bool Get(size_t bit) const {
        MKQL_ENSURE(bit < Size(), "bit index out of bounds");
        return Chunks_[bit / BitsPerChunk] & (ui64{1} << (bit % BitsPerChunk));
    }

    void Set(size_t bit, bool value = true) {
        MKQL_ENSURE(bit < Size(), "bit index out of bounds");
        ui64& chunk = Chunks_[bit / BitsPerChunk];
        const ui64 mask = ui64{1} << (bit % BitsPerChunk);
        if (value) {
            chunk |= mask;
        } else {
            chunk &= ~mask;
        }
    }

    bool Empty() const {
        return Chunks_.empty();
    }

    size_t Size() const {
        return Chunks_.size() * BitsPerChunk;
    }

    void Reset() {
        TMKQLVector<ui64>().swap(Chunks_);
    }

    const TMKQLVector<ui64>& Chunks() const {
        return Chunks_;
    }

    TMKQLVector<ui64>& Chunks() {
        return Chunks_;
    }

  private:
    static constexpr size_t BitsPerChunk = sizeof(ui64) * 8;
    TMKQLVector<ui64> Chunks_;
};

} // namespace NKikimr::NMiniKQL
