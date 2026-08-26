#pragma once
#include "defs.h"

#include <ydb/library/actors/util/rope.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <algorithm>
#include <utility>
#include "interval_set.h"

namespace NKikimr {

class TFragmentedBuffer {
    // Fragments sorted by offset, non-overlapping and never adjacent-and-joinable (Write()
    // coalesces them). One fragment is the overwhelmingly common case -- the Put path stores
    // exactly one via SetMonolith -- so keep it inline and never touch the heap. Fragment counts
    // stay tiny even on partial-range Gets, so a contiguous array beats a tree everywhere.
    TStackVec<std::pair<ui32, TRope>, 1> BufferForOffset;

    // Index of the first fragment with offset > begin, mirroring TMap::upper_bound.
    size_t UpperBound(ui32 begin) const {
        return std::upper_bound(BufferForOffset.begin(), BufferForOffset.end(), begin,
            [](ui32 value, const auto& item) { return value < item.first; }) - BufferForOffset.begin();
    }

public:
    bool IsMonolith() const;
    TRope GetMonolith();
    void SetMonolith(TRope&& data);

    void Write(ui32 begin, const char* buffer, ui32 size);
    void Write(ui32 begin, TRope&& data);
    void Read(ui32 begin, char* buffer, ui32 size) const;
    TRope Read(ui32 begin, ui32 size) const;
    TString Print() const;

    void CopyFrom(const TFragmentedBuffer& from, const TIntervalSet<i32>& range, i32 offset = 0);
    TIntervalSet<i32> GetIntervalSet() const;

    explicit operator bool() const {
        return !BufferForOffset.empty();
    }

    size_t GetTotalSize() const {
        size_t res = 0;
        for (const auto& [offset, buffer] : BufferForOffset) {
            res += buffer.size();
        }
        return res;
    }
};

} // NKikimr

