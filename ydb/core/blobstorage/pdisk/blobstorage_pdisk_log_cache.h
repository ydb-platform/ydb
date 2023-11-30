#pragma once
#include "defs.h"
#include <ydb/library/actors/util/rc_buf.h>
#include <util/generic/intrlist.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NPDisk {

/**
 * Key-value LRU cache without automatic eviction, but able to erase range of keys.
 **/
class TLogCache {
public:
    struct TCacheRecord {
        ui64 Offset = 0;
        TRcBuf Data;
        TVector<ui64> BadOffsets;

        TCacheRecord() = default;
        TCacheRecord(TCacheRecord&&);
        TCacheRecord(ui64 offset, TRcBuf data, TVector<ui64> badOffsets);
    };

private:
    struct TItem : public TIntrusiveListItem<TItem> {
        TCacheRecord Value;

        // custom constructors ignoring TIntrusiveListItem
        TItem(TItem&& other);
        explicit TItem(TCacheRecord&& value);
    };

    using TListType = TIntrusiveList<TItem>;
    using TIndex = TMap<ui64, TItem>;

public:
    size_t Size() const;
    const TCacheRecord* Find(ui64 offset);
    const TCacheRecord* FindWithoutPromote(ui64 offset) const;

    bool Pop();
    bool Insert(TCacheRecord&& value);
    size_t Erase(ui64 offset);
    size_t EraseRange(ui64 begin, ui64 end);  // erases range [begin, end)
    void Clear();

private:
    TListType List;
    TIndex Index;
};

} // NPDisk
} // NKikimr
