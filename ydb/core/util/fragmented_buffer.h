#pragma once
#include "defs.h"

#include <library/cpp/actors/util/rope.h>
#include <util/generic/map.h>
#include "interval_set.h"

namespace NKikimr {

class TFragmentedBuffer {
    TMap<i32, TRope> BufferForOffset;
    void Insert(i32 begin, const char* source, i32 bytesToCopy);

public:
    TFragmentedBuffer();

    bool IsMonolith() const;
    TRope GetMonolith();
    void SetMonolith(TRope &data);

    void Write(i32 begin, const char* buffer, i32 size);
    void Read(i32 begin, char* buffer, i32 size) const;
    TString Print() const;

    std::pair<const char*, i32> Get(i32 begin) const;
    void CopyFrom(const TFragmentedBuffer& from, const TIntervalSet<i32>& range);

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

