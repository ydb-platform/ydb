#pragma once
#include "defs.h"

#include <ydb/library/actors/util/rope.h>
#include <util/generic/map.h>
#include "interval_set.h"

namespace NKikimr {

class TFragmentedBuffer {
    TMap<ui32, TRope> BufferForOffset;

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

