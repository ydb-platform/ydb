#pragma once

#include "defs.h"
#include "query_stat_yield.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

#include <util/stream/length.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDbStatRecordMerger
    // Passes every physical record for the current key to a DB-stat aggregator.
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec>
    class TDbStatRecordMerger {
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;

    public:
        explicit TDbStatRecordMerger(TAggr* aggr)
            : Aggr(aggr)
        {}

        void AddFromFresh(const TMemRec& memRec, const TRope* data, const TKey& key, ui64 lsn) {
            Aggr->UpdateFreshRecord(memRec, data, key, lsn);
        }

        void AddFromSegment(const TMemRec& memRec, const TDiskPart* outbound, const TKey& key,
                ui64 circaLsn, const TLevelSegment* sst) {
            Aggr->UpdateLevelRecord(memRec, outbound, key, circaLsn, sst);
        }

        static constexpr bool HaveToMergeData() {
            return false;
        }

    private:
        TAggr* Aggr;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TraverseDbWithoutMerge
    // Traversing LevelIndex Database per fresh segment, per Sst, usefull
    // for gathering info disregarding garbage collection
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec>
    void TraverseDbWithoutMerge(
            const TIntrusivePtr<THullCtx> &hullCtx,
            TAggr *aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> &snap)
    {
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;

        // Fresh Segment Traversal Function
        auto traverseFreshSeg = [&] (const char *segName, const TFreshSegmentSnapshot &seg) {
            using TIterator = typename TFreshSegmentSnapshot::TIteratorWOMerge;
            TIterator it(hullCtx, &seg);
            it.SeekToFirst();
            while (it.Valid()) {
                aggr->UpdateFresh(segName, it.GetUnmergedKey(), it.GetUnmergedMemRec());
                it.Next();
            }
        };


        // Traverse Fresh
        traverseFreshSeg("FCur", snap.FreshSnap.Cur);
        traverseFreshSeg("FDreg", snap.FreshSnap.Dreg);
        traverseFreshSeg("FOld", snap.FreshSnap.Old);

        // Traverse SSTs
        TSstIterator it(&snap.SliceSnap);
        it.SeekToFirst();
        while (it.Valid()) {
            TLevelSstPtr p = it.Get();
            TMemIterator c(p.SstPtr.Get());
            c.SeekToFirst();
            while (c.Valid()) {
                aggr->UpdateLevel(p, c.GetCurKey(), c.GetMemRec());
                c.Next();
            }
            it.Next();
        }

        aggr->Finish();
    }

    ////////////////////////////////////////////////////////////////////////////
    // TraverseDbWithoutMerge with yielding
    //
    // Traverses all physical records in reverse key order. Every quantum
    // processes at least one complete key before checking whether to yield.
    // The returned key is independent of a snapshot and can therefore be used
    // to resume after the old snapshot is released and a new one is acquired.
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec, class TShouldStop>
    std::optional<TDbStatYieldedState<TKey, TMemRec>> TraverseDbWithoutMergeImpl(
            const TIntrusivePtr<THullCtx>& hullCtx,
            TAggr* aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>& snap,
            std::optional<TDbStatYieldedState<TKey, TMemRec>> yieldedState,
            std::optional<TDbStatYieldPolicy> yieldPolicy,
            TShouldStop&& shouldStop,
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
    {
        using TYieldedState = TDbStatYieldedState<TKey, TMemRec>;
        using TBackwardIterator = typename TLevelIndexSnapshot<TKey, TMemRec>::TBackwardIterator;

        TBackwardIterator iterator(hullCtx, &snap);
        THeapIterator<TKey, TMemRec, false> heap(&iterator);
        if (yieldedState) {
            heap.Seek(yieldedState->LastProcessedKey);
            // The saved key was completely processed in the previous quantum.
            if (heap.Valid() && heap.GetCurKey() == yieldedState->LastProcessedKey) {
                heap.Prev();
            }
        } else {
            heap.SeekToLast();
        }

        TDbStatRecordMerger<TAggr, TKey, TMemRec> merger(aggr);
        TDbStatYieldChecker yieldChecker(std::move(yieldPolicy), std::move(monotonicTimeProvider));
        while (heap.Valid()) {
            const TKey key = heap.GetCurKey();
            aggr->BeginKey(key);
            heap.PutToMergerAndAdvance(&merger);
            aggr->FinishKey(key);

            if (heap.Valid() && shouldStop()) {
                return TYieldedState{key};
            }

            // Do not yield after the final key; finish in the current quantum.
            if (heap.Valid() && yieldChecker.StepAndCheckForYield()) {
                return TYieldedState{key};
            }
        }

        aggr->Finish();
        return std::nullopt;
    }

    template <class TAggr, class TKey, class TMemRec>
    std::optional<TDbStatYieldedState<TKey, TMemRec>> TraverseDbWithoutMerge(
            const TIntrusivePtr<THullCtx>& hullCtx,
            TAggr* aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>& snap,
            std::optional<TDbStatYieldedState<TKey, TMemRec>> yieldedState,
            std::optional<TDbStatYieldPolicy> yieldPolicy,
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
    {
        return TraverseDbWithoutMergeImpl(
            hullCtx,
            aggr,
            snap,
            std::move(yieldedState),
            std::move(yieldPolicy),
            [] { return false; },
            std::move(monotonicTimeProvider));
    }

    template <class TAggr, class TKey, class TMemRec, class TShouldStop>
    std::optional<TDbStatYieldedState<TKey, TMemRec>> TraverseDbWithoutMergeUntil(
            const TIntrusivePtr<THullCtx>& hullCtx,
            TAggr* aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>& snap,
            std::optional<TDbStatYieldedState<TKey, TMemRec>> yieldedState,
            std::optional<TDbStatYieldPolicy> yieldPolicy,
            TShouldStop&& shouldStop,
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
    {
        // Like the time-based variant above, the stop predicate is checked only
        // after all physical records for the current key have been processed.
        return TraverseDbWithoutMergeImpl(
            hullCtx,
            aggr,
            snap,
            std::move(yieldedState),
            std::move(yieldPolicy),
            std::forward<TShouldStop>(shouldStop),
            std::move(monotonicTimeProvider));
    }

    ////////////////////////////////////////////////////////////////////////////
    // TDbDumper
    // The class makes a dump of a Hull Database for introspection purposes
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TDbDumper {
    private:
        using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;

    public:
        struct TConstraint {
            ui64 TabletId;
            ui32 Channel;

            TConstraint(ui64 tabletId, ui32 channel)
                : TabletId(tabletId)
                , Channel(channel)
            {}

            // default implementation
            bool Check(const TKey &key) const {
                Y_UNUSED(key);
                return true;
            }

            void Output(IOutputStream &str) const {
                str << "{TabletId# " << TabletId << " Channel# " << Channel << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

    private:
        class TDumpRecordMerger {
            TCountingOutput& Str;
            const char *Marker;
            TIntrusivePtr<THullCtx> HullCtx;
            TKey CurKey;
            const TString Prefix;
            const TMaybe<TConstraint> Constraint;

        public:
            TDumpRecordMerger(TCountingOutput& str,
                              const char *marker,
                              TIntrusivePtr<THullCtx> hullCtx,
                              const TString &prefix,
                              TMaybe<TConstraint> constraint)
                : Str(str)
                , Marker(marker)
                , HullCtx(std::move(hullCtx))
                , Prefix(prefix)
                , Constraint(constraint)
            {}

            void SetCurKey(const TKey& curKey) {
                CurKey = curKey;
            }

            void AddFromFresh(const TMemRec& memRec, const TRope* /*data*/, const TKey& key, ui64 /*lsn*/) {
                Y_VERIFY_S(key == CurKey, HullCtx->VCtx->VDiskLogPrefix);
                if (!Constraint || Constraint->Check(CurKey)) {
                    auto mr = memRec.ToString(HullCtx->IngressCache.Get(), nullptr);
                    auto ing = IngressToString(HullCtx->VCtx->Top.get(), HullCtx->VCtx->ShortSelfVDisk, CurKey, memRec);
                    Str << Prefix
                        << Marker
                        << " Key: " << CurKey.ToString()
                        << " Ingress: " << ing
                        << " MemRec: " << mr
                        << "\n";
                }
            }

            void Finish()
            {}

            void Clear()
            {}

            bool HaveToMergeData() const {
                return false;
            }
        };

        void DumpFreshSegment(TCountingOutput &str,
                              TMaybe<typename TFreshSegmentSnapshot::TForwardIterator>& it,
                              const char *marker) {
            if (!it) {
                return;
            }

            TDumpRecordMerger merger(str, marker, HullCtx, Prefix, Constraint);
            while (it->Valid()) {
                // check limit
                if (str.Counter() >= LimitInBytes) {
                    return;
                }
                merger.SetCurKey(it->GetCurKey());
                it->PutToMerger(&merger);
                merger.Finish();
                merger.Clear();
                it->Next();
            }
            it.Clear();
        }

        void DumpFresh(TCountingOutput &str) {
            DumpFreshSegment(str, FCurIt, "FCur");
            DumpFreshSegment(str, FDregIt, "FDreg");
            DumpFreshSegment(str, FOldIt, "FOld");
        }

        // move SstIt/MemIt to next valid item; on exit either SstIt and MemIt are valid, nor SstIt is not valid
        void AdjustMemIt() {
            while (SstIt->Valid()) {
                TLevelSstPtr p = SstIt->Get();
                MemIt = TMemIterator(p.SstPtr.Get());
                MemIt->SeekToFirst();
                if (MemIt->Valid()) {
                    break;
                } else {
                    SstIt->Next();
                }
            }
        }

        void DumpLevels(TCountingOutput &str) {
            while (SstIt->Valid()) {
                const auto& p = SstIt->Get();
                while (MemIt->Valid()) {
                    // check limit
                    if (str.Counter() >= LimitInBytes) {
                        return;
                    }
                    const auto& c = *MemIt;
                    if (!Constraint || Constraint->Check(c.GetCurKey())) {
                        auto mr = c.GetMemRec().ToString(HullCtx->IngressCache.Get(), c.GetSstPtr()->GetOutbound());
                        auto ing = IngressToString(HullCtx->VCtx->Top.get(), HullCtx->VCtx->ShortSelfVDisk,
                                c.GetCurKey(), c.GetMemRec());
                        str << Prefix
                            << "L: " << p.Level
                            << " ID: " << p.SstPtr->AssignedSstId
                            << " Key: " << c.GetCurKey().ToString()
                            << " Ingress: " << ing
                            << " MemRec: " << mr
                            << "\n";
                    }
                    MemIt->Next();
                }
                SstIt->Next();
                AdjustMemIt();
            }
        }

        // default implementation
        static TString IngressToString(const TBlobStorageGroupInfo::TTopology *top,
                                       const TVDiskIdShort &vdisk,
                                       const TKey &key,
                                       const TMemRec &memRec) {
            Y_UNUSED(top);
            Y_UNUSED(vdisk);
            Y_UNUSED(key);
            Y_UNUSED(memRec);
            return TString();
        }

    public:
        TDbDumper(TIntrusivePtr<THullCtx> hullCtx,
                  TLevelIndexSnapshot &&snapshot,
                  ui64 limitInBytes = Max<ui64>(),
                  const TString &prefix = TString(),
                  TMaybe<TConstraint> constraint = {})
            : HullCtx(std::move(hullCtx))
            , Snapshot(std::move(snapshot))
            , LimitInBytes(limitInBytes)
            , Prefix(prefix)
            , Constraint(constraint)
        {
            FCurIt.ConstructInPlace(HullCtx, &Snapshot.FreshSnap.Cur);
            FCurIt->SeekToFirst();
            FDregIt.ConstructInPlace(HullCtx, &Snapshot.FreshSnap.Dreg);
            FDregIt->SeekToFirst();
            FOldIt.ConstructInPlace(HullCtx, &Snapshot.FreshSnap.Old);
            FOldIt->SeekToFirst();

            SstIt.ConstructInPlace(&Snapshot.SliceSnap);
            SstIt->SeekToFirst();
            AdjustMemIt();
        }

        enum class EDumpRes {
            OK = 0,
            Limited = 1
        };

        // The method dumps database to str.
        // It can limit output size close to 'limitInBytes', to avoid
        // dumping too much.
        EDumpRes Dump(IOutputStream &str) {
            TCountingOutput countedStr(&str);
            DumpFresh(countedStr);
            DumpLevels(countedStr);
            return (countedStr.Counter() >= LimitInBytes) ? EDumpRes::Limited : EDumpRes::OK;
        }

        bool Done() const {
            return !FCurIt && !FDregIt && !FOldIt && !SstIt->Valid();
        }

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        TLevelIndexSnapshot Snapshot;
        // Limit in bytes on output stream
        const ui64 LimitInBytes;
        // Prefix for the line, empty by default
        const TString Prefix;
        // We may dump using this constraint (only specific tabletId and channel)
        const TMaybe<TConstraint> Constraint;

        // Reentrant state
        TMaybe<typename TFreshSegmentSnapshot::TForwardIterator> FCurIt, FDregIt, FOldIt;
        TMaybe<TSstIterator> SstIt;
        TMaybe<TMemIterator> MemIt;
    };

    template <>
    inline TString
    TDbDumper<TKeyLogoBlob, TMemRecLogoBlob>::IngressToString(const TBlobStorageGroupInfo::TTopology *top,
                                                              const TVDiskIdShort &vdisk,
                                                              const TKeyLogoBlob &key,
                                                              const TMemRecLogoBlob &memRec) {
        return memRec.GetIngress().ToString(top, vdisk, key.LogoBlobID());
    }

    // specialization for TDbDumper::TConstraint::Check
    template <>
    inline bool
    TDbDumper<TKeyLogoBlob, TMemRecLogoBlob>::TConstraint::Check(const TKeyLogoBlob &key) const {
        const auto &id = key.LogoBlobID();
        return id.TabletID() == TabletId && id.Channel() == Channel;
    }

    template <>
    inline bool
    TDbDumper<TKeyBarrier, TMemRecBarrier>::TConstraint::Check(const TKeyBarrier &key) const {
        return key.TabletId == TabletId && key.Channel == Channel;
    }

    template <>
    inline bool
    TDbDumper<TKeyBlock, TMemRecBlock>::TConstraint::Check(const TKeyBlock &key) const {
        return key.TabletId == TabletId;
    }

} // NKikimr
