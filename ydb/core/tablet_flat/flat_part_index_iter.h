#pragma once

#include "flat_part_iface.h"
#include "flat_page_index.h"
#include "flat_table_part.h"


namespace NKikimr::NTable {

class TPartIndexIt {
public:
    using TCells = NPage::TCells;
    using TRecord = NPage::TIndex::TRecord;
    using TIndex = NPage::TIndex;
    using TIter = NPage::TIndex::TIter;
    using TGroupId = NPage::TGroupId;

    TPartIndexIt(const TPart* part, TGroupId groupId)
        : Part(part)
        , GroupId(groupId)
        , EndRowId(part->GetGroupIndex(groupId).GetEndRowId())
    { }
    
    EReady Seek(TRowId rowId, bool restart = false) {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupRow(rowId, restart ? TIter() : Iter);
        return DataOrGone();
    }

    EReady Seek(TCells key, ESeek seek, const TPartScheme::TGroupInfo &scheme, const TKeyCellDefaults *keyDefaults) {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupKey(key, scheme, seek, keyDefaults);
        return DataOrGone();
    }

    EReady SeekReverse(TCells key, ESeek seek, const TPartScheme::TGroupInfo &scheme, const TKeyCellDefaults *keyDefaults) {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupKeyReverse(key, scheme, seek, keyDefaults);
        return DataOrGone();
    }

    EReady Next() {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }
        Iter++;
        return DataOrGone();
    }

    EReady Prev() {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }
        if (Iter.Off() == 0) {
            return EReady::Gone;
        }
        Iter--;
        return DataOrGone();
    }

    bool IsValid() {
        return bool(Iter);
    }

public:
    TRowId GetEndRowId() {
        return EndRowId;
    }

    TPageId GetPageId() {
        Y_VERIFY(Index);
        return Iter->GetPageId();
    }

    TRowId GetRowId() {
        Y_VERIFY(Index);
        return Iter->GetRowId();
    }
    
    TRowId GetNextRowId() {
        Y_VERIFY(Index);
        auto next = Iter + 1;
        return next
            ? next->GetRowId()
            : Max<TRowId>();
    }
    const TRecord * GetRecord() {
        Y_VERIFY(Index);
        return Iter.GetRecord();
    }

private:
    EReady DataOrGone() {
        return Iter ? EReady::Data : EReady::Gone;
    }

    TIndex* TryGetIndex() {
        if (Index) {
            return &*Index;
        }
        // TODO: get index from Env
        Index = Part->GetGroupIndex(GroupId);
        return &*Index;
    }

private:
    const TPart* const Part;
    const TGroupId GroupId;
    std::optional<TIndex> Index;
    TIter Iter;
    TRowId EndRowId;
};

}
