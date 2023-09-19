#pragma once

#include "flat_part_iface.h"
#include "flat_page_index.h"
#include "flat_table_part.h"
#include <ydb/library/yverify_stream/yverify_stream.h>


namespace NKikimr::NTable {

class TPartIndexIt {
public:
    using TCells = NPage::TCells;
    using TRecord = NPage::TIndex::TRecord;
    using TIndex = NPage::TIndex;
    using TIter = NPage::TIndex::TIter;
    using TGroupId = NPage::TGroupId;

    TPartIndexIt(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        // Note: EndRowId may be Max<TRowId>() for legacy TParts
        , EndRowId(groupId.IsMain() && part->Stat.Rows ? part->Stat.Rows : Max<TRowId>())
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
            Iter = { };
            return EReady::Gone;
        }
        Iter--;
        return DataOrGone();
    }

    bool IsValid() {
        return bool(Iter);
    }

    // for precharge only
    TIndex* TryLoadRaw() {
        return TryGetIndex();
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
        auto pageId = GroupId.IsHistoric() ? Part->IndexPages.Historic[GroupId.Index] : Part->IndexPages.Groups[GroupId.Index];
        auto page = Env->TryGetPage(Part, pageId);
        if (page) {
            Index = TIndex(*page);
            Y_VERIFY_DEBUG_S(EndRowId == Index->GetEndRowId(), "EndRowId mismatch " << EndRowId << " != " << Index->GetEndRowId() << " (group " << GroupId.Historic << "/" << GroupId.Index <<")");
            return &*Index;
        }
        return { };
    }

private:
    const TPart* const Part;
    IPages* const Env;
    const TGroupId GroupId;
    std::optional<TIndex> Index;
    TIter Iter;
    TRowId EndRowId;
};

}
