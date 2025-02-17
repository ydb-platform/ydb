#pragma once

#include "flat_part_iface.h"
#include "flat_page_flat_index.h"
#include "flat_part_index_iter_iface.h"
#include "flat_table_part.h"
#include "flat_stat_part_group_iter_iface.h"
#include <ydb/library/yverify_stream/yverify_stream.h>


namespace NKikimr::NTable {

class TPartGroupFlatIndexIter : public IPartGroupIndexIter, public IStatsPartGroupIter {
public:
    using TCells = NPage::TCells;
    using TRecord = NPage::TFlatIndex::TRecord;
    using TIndex = NPage::TFlatIndex;
    using TIter = NPage::TFlatIndex::TIter;
    using TGroupId = NPage::TGroupId;

    TPartGroupFlatIndexIter(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        // Note: EndRowId may be Max<TRowId>() for legacy TParts
        , EndRowId(groupId.IsMain() && part->Stat.Rows ? part->Stat.Rows : Max<TRowId>())
    { }
    
    EReady Start() override {
        return Seek(0);
    }

    EReady Seek(TRowId rowId) override {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupRow(rowId, Iter);
        return DataOrGone();
    }

    EReady SeekLast() override {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }
        Iter = (*index)->End();
        if (Iter.Off() == 0) {
            return EReady::Gone;
        }
        Iter--;
        return DataOrGone();
    }

    EReady Seek(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) override {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupKey(key, GroupInfo, seek, keyDefaults);
        return DataOrGone();
    }

    EReady SeekReverse(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) override {
        auto index = TryGetIndex();
        if (!index) {
            return EReady::Page;
        }

        Iter = index->LookupKeyReverse(key, GroupInfo, seek, keyDefaults);
        return DataOrGone();
    }

    EReady Next() override {
        Y_DEBUG_ABORT_UNLESS(Index);
        Y_DEBUG_ABORT_UNLESS(Iter);
        Iter++;
        return DataOrGone();
    }

    EReady Prev() override {
        Y_DEBUG_ABORT_UNLESS(Index);
        Y_DEBUG_ABORT_UNLESS(Iter);
        if (Iter.Off() == 0) {
            Iter = { };
            return EReady::Gone;
        }
        Iter--;
        return DataOrGone();
    }

    bool IsValid() const override {
        Y_DEBUG_ABORT_UNLESS(Index);
        return bool(Iter);
    }

    void AddLastDeltaDataSize(TChanneledDataSize& dataSize) override {
        Y_DEBUG_ABORT_UNLESS(Index);
        Y_DEBUG_ABORT_UNLESS(Iter.Off());
        TPageId pageId = (Iter - 1)->GetPageId();
        ui64 delta = Part->GetPageSize(pageId, GroupId);
        ui8 channel = Part->GetGroupChannel(GroupId);
        dataSize.Add(delta, channel);
    }

    // for precharge and TForward only
    TIndex* TryLoadRaw() {
        return TryGetIndex();
    }

public:
    TRowId GetEndRowId() const override {
        return EndRowId;
    }

    TPageId GetPageId() const override {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        return Iter->GetPageId();
    }

    TRowId GetRowId() const override {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        return Iter->GetRowId();
    }

    TRowId GetNextRowId() const override {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        auto next = Iter + 1;
        return next
            ? next->GetRowId()
            : EndRowId;
    }

    TPos GetKeyCellsCount() const override {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        return GroupInfo.KeyTypes.size();
    }

    TCell GetKeyCell(TPos index) const override {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        return Iter.GetRecord()->Cell(GroupInfo.ColsKeyIdx[index]);
    }

    void GetKeyCells(TSmallVec<TCell>& keyCells) const override {
        keyCells.clear();

        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        
        auto record = Iter.GetRecord();
        for (auto index : xrange(GroupInfo.KeyTypes.size())) {
            keyCells.push_back(record->Cell(GroupInfo.ColsKeyIdx[index]));
        }
    }

    const TRecord * GetRecord() const {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter);
        return Iter.GetRecord();
    }

    // currently this method is needed for tests only, but it's worth to keep it for future optimizations
    const TRecord * GetLastRecord() const {
        Y_ABORT_UNLESS(Index);
        Y_ABORT_UNLESS(Iter, "Should be called only after SeekLast call");
        return Index->GetLastKeyRecord();
    }

private:
    EReady DataOrGone() const {
        return Iter ? EReady::Data : EReady::Gone;
    }

    TIndex* TryGetIndex() {
        if (Index) {
            return &*Index;
        }
        auto pageId = Part->IndexPages.GetFlat(GroupId);
        auto page = Env->TryGetPage(Part, pageId, {});
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
    const TPartScheme::TGroupInfo& GroupInfo;
    std::optional<TIndex> Index;
    TIter Iter;
    TRowId EndRowId;
};

}
