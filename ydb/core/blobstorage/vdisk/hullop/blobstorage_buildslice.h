#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_leveledssts.h>
#include <ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_defs.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullOpUtil -- utils for compaction
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct THullOpUtil {
        using TCompactionTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
        using TFreshSegment = ::NKikimr::TFreshSegment<TKey, TMemRec>;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using TLevelSlice = ::NKikimr::TLevelSlice<TKey, TMemRec>;
        using TLevelSlicePtr = TIntrusivePtr<TLevelSlice>;
        using TSliceSstIterator = typename TLevelSlice::TSstIterator;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
        using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;
        using TLeveledSsts = ::NKikimr::TLeveledSsts<TKey, TMemRec>;
        using TLeveledSstsIterator = typename TLeveledSsts::TIterator;
        using TSortedLevel = ::NKikimr::TSortedLevel<TKey, TMemRec>;

        // Find removed huge blobs
        static TDiskPartVec FindRemovedHugeBlobsAfterLevelCompaction(
                const TActorContext &ctx,
                TSliceSstIterator oldSliceIt,
                const TLevelSlice *newSlice);

        static TDiskPartVec FindRemovedHugeBlobsAfterFreshCompaction(
                const TActorContext &ctx,
                const TIntrusivePtr<TFreshSegment> &freshSeg,
                const TOrderedLevelSegmentsPtr &segVec);

        struct TBuiltSlice {
            // new slice built from the previous one and changes provided
            TLevelSlicePtr NewSlice;
            // calculated removed huge blobs based on slice comparison,
            // for debug purposes only
            TDiskPartVec RemovedHugeBlobs;
        };

        // Build new slice after compaction from previous slice and modifications provided
        static TBuiltSlice BuildSlice(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TActorContext &ctx,
                const TLevelIndexSettings &settings,
                const TLevelSlice *slice,
                const TCompactionTask &ctask,
                bool findRemovedHugeBlobs);

    };

//#define HULL_COMPACT_APPLY

    template <class TKey, class TMemRec>
    typename THullOpUtil<TKey, TMemRec>::TBuiltSlice
    THullOpUtil<TKey, TMemRec>::BuildSlice(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TActorContext &ctx,
            const TLevelIndexSettings &settings,
            const TLevelSlice *slice,
            const TCompactionTask &ctask,
            bool findRemovedHugeBlobs)
    {
        Y_UNUSED(ctx); // for log in debug
        // initialize iterator for adding/removing
        TLeveledSstsIterator addIt(&ctask.GetSstsToAdd());
        addIt.SeekToFirst();
        TLeveledSstsIterator delIt(&ctask.GetSstsToDelete());
        delIt.SeekToFirst();
        // create a new slice
        TLevelSlicePtr res(new TLevelSlice(settings, slice->Ctx));

        ui32 levelsSize = slice->SortedLevels.size();
        if (ctask.Action == NHullComp::ActCompactSsts &&
            ctask.CompactSsts.TargetLevel != ui32(-1) &&
            ctask.CompactSsts.TargetLevel > levelsSize) {
            // resize levels
            levelsSize = ctask.CompactSsts.TargetLevel;
        }

        res->SortedLevels.reserve(levelsSize);
        for (ui32 j = 0; j < levelsSize; j++) {
            // update/copy LastCompactedKey
            TKey lastCompactedKey;
            if (ctask.Action == NHullComp::ActCompactSsts && j == ctask.CompactSsts.TargetLevel) {
                lastCompactedKey = ctask.CompactSsts.LastCompactedKey;
            } else if (j < slice->SortedLevels.size()) {
                lastCompactedKey = slice->SortedLevels[j].LastCompactedKey;
            } else {
                lastCompactedKey = TKey::First();
            }
            res->SortedLevels.push_back(TSortedLevel(lastCompactedKey));
        }

        // prepare iterators
        // NOTE: we use current number of ssts for Level 0 (slice->Level0CurSstsNum()). Number of ssts at Level 0
        //       could change during compaction, but we have to use the actual value to avoid loosing
        //       Level 0 ssts we got while compaction was running
        TSliceSstIterator sliceIt(slice, slice->Level0CurSstsNum());
        sliceIt.SeekToFirst();

        auto checkOrder = [](auto iter) {
            if (iter.Valid()) {
                auto prev = iter.Get();
                for (iter.Next(); iter.Valid(); iter.Next()) {
                    auto current = iter.Get();
                    if (current < prev) {
                        return false;
                    } else {
                        prev = current;
                    }
                }
            }
            return true;
        };
        auto dump = [](auto iter) {
            TStringStream str;
            str << "{";
            bool first = true;
            for (; iter.Valid(); iter.Next()) {
                if (first) {
                    first = false;
                } else {
                    str << " ";
                }
                str << iter.Get().ToString();
            }
            str << "}";
            return str.Str();
        };
        Y_ABORT_UNLESS(checkOrder(addIt), "addIt# %s", dump(addIt).data());
        Y_ABORT_UNLESS(checkOrder(delIt), "delIt# %s", dump(delIt).data());
        Y_ABORT_UNLESS(checkOrder(sliceIt), "sliceIt# %s", dump(sliceIt).data());

#ifdef HULL_COMPACT_APPLY
        TStringStream debugOutput;
        {
            debugOutput << "COMPACTION_APPLY\n";
            debugOutput << " ORIG\n" << slice->ToString("  ");
            debugOutput << " CHANGES\n  DELETE: ";
            TLeveledSstsIterator d(delIt);
            d.SeekToFirst();
            while (d.Valid()) {
                debugOutput << "[Level: " << d.Get().Level << " " << d.Get().SstPtr->ChunksToString() << "]";
                d.Next();
            }
            debugOutput << "\n ADD: ";
            TLeveledSstsIterator v(addIt);
            v.SeekToFirst();
            while (v.Valid()) {
                debugOutput << "[Level: " << v.Get().Level << " " << v.Get().SstPtr->ChunksToString() << "]";
                v.Next();
            }
            debugOutput << "\n COMMANDS\n";
        }
#endif

        // merge
        while (sliceIt.Valid() && addIt.Valid()) {
            if (sliceIt.Get() < addIt.Get()) {
#ifdef HULL_COMPACT_APPLY
                debugOutput << "  SLICE_LESS(1) sliceIt: " << sliceIt.Get().ToString() << "\n";
#endif
                if (delIt.Valid() && sliceIt.Get().IsSameSst(delIt.Get())) {
                    delIt.Next();
                } else {
                    TLevelSstPtr item = sliceIt.Get();
                    res->Put(item);
#ifdef HULL_COMPACT_APPLY
                    debugOutput << "  Put Item(1): " << item.ToString() << "\n";
#endif
                }
                sliceIt.Next();
            } else if (addIt.Get() < sliceIt.Get()) {
                TLevelSstPtr item = addIt.Get();
                res->Put(item);
#ifdef HULL_COMPACT_APPLY
                debugOutput << "  VEC_LESS(1)   sliceIt: " << item.ToString() << "\n";
                debugOutput << "  Put Item(2): " << item.ToString() << "\n";
#endif
                addIt.Next();
            } else {
                // do smth
#ifdef HULL_COMPACT_APPLY
                debugOutput << "  BOTH        sliceIt: " << sliceIt.Get().ToString() << " addIt: "
                            << addIt.Get().ToString() << "\n";
#endif
                Y_ABORT_UNLESS(delIt.Valid() && sliceIt.Get().IsSameSst(delIt.Get()));
                delIt.Next();
                sliceIt.Next();
                TLevelSstPtr item = addIt.Get();
                res->Put(item);
#ifdef HULL_COMPACT_APPLY
                debugOutput << "  Put Item(3): " << item.ToString() << "\n";
#endif
                addIt.Next();
            }
        }

        while (sliceIt.Valid()) {
#ifdef HULL_COMPACT_APPLY
            debugOutput << "  SLICE_LESS(2) sliceIt: " << sliceIt.Get().ToString() << "\n";
#endif
            if (delIt.Valid() && sliceIt.Get().IsSameSst(delIt.Get())) {
                delIt.Next();
            } else {
                TLevelSstPtr item = sliceIt.Get();
                res->Put(item);
#ifdef HULL_COMPACT_APPLY
                debugOutput << "  Put Item(4): " << item.ToString() << "\n";
#endif
            }
            sliceIt.Next();
        }

        while (addIt.Valid()) {
            TLevelSstPtr item = addIt.Get();
            res->Put(item);
#ifdef HULL_COMPACT_APPLY
            debugOutput << "  VEC_LESS(2)   sliceIt: " << item.ToString() << "\n";
            debugOutput << "  Put Item(5): " << item.ToString() << "\n";
#endif
            addIt.Next();
        }

        Y_ABORT_UNLESS(!sliceIt.Valid());
        Y_ABORT_UNLESS(!addIt.Valid());
        Y_ABORT_UNLESS(!delIt.Valid()); // ensure we didn't miss something

#ifdef HULL_COMPACT_APPLY
        debugOutput << " RESULT\n" << res->ToString("  ");
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLCOMP, VDISKP(vctx, "%s", ~debugOutput.Str()));
#else
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLCOMP, VDISKP(vctx, "Changes to Hull applied"));
#endif

        TSliceSstIterator resIt(res.Get(), res->Level0CurSstsNum());
        resIt.SeekToFirst();
        Y_ABORT_UNLESS(checkOrder(resIt), "resIt# %s", dump(resIt).data());


        // additional check
        TDiskPartVec removedHugeBlobs;
        if (findRemovedHugeBlobs) {
            removedHugeBlobs = FindRemovedHugeBlobsAfterLevelCompaction(ctx, sliceIt, res.Get());
        }

        return TBuiltSlice{ res, removedHugeBlobs };
    }

    template <class TKey, class TMemRec>
    TDiskPartVec
    THullOpUtil<TKey, TMemRec>::FindRemovedHugeBlobsAfterLevelCompaction(
            const TActorContext &ctx,
            TSliceSstIterator oldSliceIt,
            const TLevelSlice *newSlice)
    {
        Y_UNUSED(ctx);
        TSet<TDiskPart> hugeBlobs;

        // find all huge blobs in slice and call func on each
        auto traverseHugeBlobs = [] (TSliceSstIterator it, auto func) {
            it.SeekToFirst();
            while (it.Valid()) {
                TLevelSstPtr p = it.Get();

                TMemIterator c(p.SstPtr.Get());
                c.SeekToFirst();
                while (c.Valid()) {
                    TBlobType::EType type = c->MemRec.GetType();
                    if (type == TBlobType::HugeBlob || type == TBlobType::ManyHugeBlobs) {
                        TDiskDataExtractor extr;
                        c->MemRec.GetDiskData(&extr, p.SstPtr->GetOutbound());
                        for (const TDiskPart *hb = extr.Begin; hb != extr.End; ++hb) {
                            func(*hb);
                        }
                    }
                    c.Next();
                }

                it.Next();
            }
        };

        auto addHugeBlob = [&hugeBlobs] (const TDiskPart &part) {
            bool inserted = hugeBlobs.insert(part).second;
            Y_ABORT_UNLESS(inserted);
        };

        auto removeHugeBlob = [&hugeBlobs] (const TDiskPart &part) {
            auto num = hugeBlobs.erase(part);
            Y_ABORT_UNLESS(num == 1, "num=%u", unsigned(num));
        };


        // collect all huge blobs from old slice into hugeBlobs set
        traverseHugeBlobs(oldSliceIt, addHugeBlob);

        // in hugeBlobs leave only those blobs, that do not present in new slice
        TSliceSstIterator newSliceIt(newSlice, newSlice->Level0CurSstsNum());
        traverseHugeBlobs(newSliceIt, removeHugeBlob);

        // put from the set to a vector
        TDiskPartVec result;
        result.Reserve(hugeBlobs.size());
        for (const auto &x : hugeBlobs) {
            result.PushBack(x);
        }
        return result;
    }

    template <class TKey, class TMemRec>
    TDiskPartVec
    THullOpUtil<TKey, TMemRec>::FindRemovedHugeBlobsAfterFreshCompaction(
            const TActorContext &ctx,
            const TIntrusivePtr<TFreshSegment> &freshSeg,
            const TOrderedLevelSegmentsPtr &segVec)
    {
        Y_UNUSED(ctx);
        TSet<TDiskPart> hugeBlobs;

        // fill in hugeBlobs with blobs from fresh segment
        freshSeg->GetHugeBlobs(hugeBlobs);


        // find all huge blobs in SST and call func on each
        auto traverseHugeBlobs = [] (const TLevelSegmentPtr &seg, auto func) {
            TMemIterator c(seg.Get());
            c.SeekToFirst();
            while (c.Valid()) {
                TBlobType::EType type = c->MemRec.GetType();
                if (type == TBlobType::HugeBlob || type == TBlobType::ManyHugeBlobs) {
                    TDiskDataExtractor extr;
                    c->MemRec.GetDiskData(&extr, seg->GetOutbound());
                    for (const TDiskPart *hb = extr.Begin; hb != extr.End; ++hb) {
                        func(*hb);
                    }
                }
                c.Next();
            }
        };

        // remove huge blob from the set
        auto removeHugeBlob = [&hugeBlobs] (const TDiskPart &part) {
            auto num = hugeBlobs.erase(part);
            Y_ABORT_UNLESS(num == 1, "num=%u", unsigned(num));
        };

        // leave only deleted huge blobs in hugeBlobs
        if (segVec) {
            for (auto &seg : segVec->Segments) {
                traverseHugeBlobs(seg, removeHugeBlob);
            }
        }

        // put from the set to a vector
        TDiskPartVec result;
        result.Reserve(hugeBlobs.size());
        for (const auto &x : hugeBlobs) {
            result.PushBack(x);
        }
        return result;
    }

} // NKikimr
