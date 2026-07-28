#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr::NTest {

    template <class TKey, class TMemRec>
    TIntrusivePtr<TLevelSegment<TKey, TMemRec>> MakeTestSst(
            const TVDiskContextPtr& vctx,
            ui64 id,
            TTrackableVector<typename TLevelSegment<TKey, TMemRec>::TRec>&& index,
            const TVector<TDiskPart>& outbound = {},
            TInstant creationTime = TInstant::Zero(),
            ui64 lsn = 1)
    {
        auto sst = MakeIntrusive<TLevelSegment<TKey, TMemRec>>(vctx);
        sst->LoadedIndex = std::move(index);
        for (const auto& part : outbound) {
            sst->LoadedOutbound.push_back(part);
        }
        sst->AssignedSstId = id;
        sst->Info.Items = sst->LoadedIndex.size();
        sst->Info.FirstLsn = sst->Info.LastLsn = lsn;
        sst->Info.CTime = creationTime;
        return sst;
    }

    enum class ETestSstLevels {
        MustExist, // throw on a level the index does not have yet
        Grow,      // append empty sorted levels up to the requested one
    };

    template <class TKey, class TMemRec>
    void PutTestSst(TLevelIndex<TKey, TMemRec>& index, ui32 level,
            TIntrusivePtr<TLevelSegment<TKey, TMemRec>>& sst,
            ETestSstLevels levels = ETestSstLevels::MustExist)
    {
        auto& slice = *index.CurSlice;
        if (level == 0) {
            slice.Level0.Put(sst);
            return;
        }
        if (levels == ETestSstLevels::Grow) {
            while (slice.SortedLevels.size() < level) {
                slice.SortedLevels.emplace_back(TKey::First());
            }
        }
        slice.SortedLevels.at(level - 1).Put(sst);
    }

} // namespace NKikimr::NTest
