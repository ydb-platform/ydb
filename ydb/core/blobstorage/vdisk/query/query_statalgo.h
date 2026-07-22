#pragma once

#include "defs.h"
#include "query_stat_yield.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

#include <util/stream/length.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TraverseFreshSegment
    // Traverses a single fresh segment. May yield mid-traversal and resume
    // from yielded position later.
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec>
    std::optional<TDbStatYeildedState<TKey, TMemRec>> TraverseFreshSegment(
            const TIntrusivePtr<THullCtx> &hullCtx,
            TAggr *aggr,
            const char *segName,
            const ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec> &seg,
            typename TDbStatYeildedState<TKey, TMemRec>::EFreshSegment segmentType,
            std::optional<typename TDbStatYeildedState<TKey, TMemRec>::TFreshIterator> resumeIt,
            TDbStatYieldChecker& yeildChecker)
    {
        using TYeildedState = TDbStatYeildedState<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TIterator = typename TFreshSegmentSnapshot::TIteratorWOMerge;

        TIterator it = resumeIt ? *resumeIt : TIterator(hullCtx, &seg);
        if (!resumeIt) {
            it.SeekToFirst();
        }

        while (it.Valid()) {
            aggr->UpdateFresh(segName, it.GetUnmergedKey(), it.GetUnmergedMemRec());
            it.Next();
            if (it.Valid() && yeildChecker.StepAndCheckForYield()) {
                return TYeildedState{typename TYeildedState::TFreshPosition{segmentType, it}};
            }
        }
        return std::nullopt;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TraverseDbWithoutMerge
    // Traversing LevelIndex Database per fresh segment, per Sst, usefull
    // for gathering info disregarding garbage collection.
    //
    // Execution may be yielded if according policy is passed and later resumed
    // from saved state. 
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec>
    std::optional<TDbStatYeildedState<TKey, TMemRec>> TraverseDbWithoutMerge(
            const TIntrusivePtr<THullCtx> &hullCtx,
            TAggr *aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> &snap,
            std::optional<TDbStatYeildedState<TKey, TMemRec>> yeildedState = std::nullopt,
            std::optional<TDbStatYieldPolicy> yeildPolicy = std::nullopt)
    {
        using TYeildedState = TDbStatYeildedState<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TYeildedState::TSstIterator;
        using TMemIterator = typename TYeildedState::TMemIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using EFreshSegment = typename TYeildedState::EFreshSegment;

        TDbStatYieldChecker yeildChecker(std::move(yeildPolicy));

        // Description of a single fresh segment to traverse
        struct TSegmentDescription {
            const char* Name;
            EFreshSegment Type;
            const TFreshSegmentSnapshot& Seg;
        };
        const TSegmentDescription segments[] = {
            {"FCur", EFreshSegment::Cur, snap.FreshSnap.Cur},
            {"FDreg", EFreshSegment::Dreg, snap.FreshSnap.Dreg},
            {"FOld", EFreshSegment::Old, snap.FreshSnap.Old},
        };

        // Figure out where to (re)start traversal
        size_t startFreshSegmentIdx = 0;
        bool resumeLevels = false;
        std::optional<typename TYeildedState::TFreshIterator> freshResumeIt;
        std::optional<TSstIterator> sstResumeIt;
        std::optional<TMemIterator> memResumeIt;

        if (yeildedState) {
            if (auto* fresh = std::get_if<typename TYeildedState::TFreshPosition>(&yeildedState->Position)) {
                startFreshSegmentIdx = static_cast<size_t>(fresh->Segment);
                freshResumeIt = fresh->Iterator;
            } else {
                auto& level = std::get<typename TYeildedState::TLevelPosition>(yeildedState->Position);
                resumeLevels = true;
                sstResumeIt = level.SstIt;
                memResumeIt = level.MemIt;
            }
        }

        // Traverse Fresh
        if (!resumeLevels) {
            for (size_t i = startFreshSegmentIdx; i < Y_ARRAY_SIZE(freshSegs); ++i) {
                const TSegmentDescription& description = freshSegs[i];
                std::optional<typename TYeildedState::TFreshIterator> resumeIt;
                if (i == startFreshSegmentIdx) {
                    resumeIt = std::move(freshResumeIt);
                }
                if (auto yielded = TraverseFreshSegment(hullCtx, aggr, description.Name, description.Seg,
                        description.Type, std::move(resumeIt), yeildChecker)) {
                    return yielded;
                }
            }
        }

        // Traverse SSTs
        TSstIterator it = resumeLevels ? *sstResumeIt : TSstIterator(&snap.SliceSnap);
        if (!resumeLevels) {
            it.SeekToFirst();
        }
        while (it.Valid()) {
            TLevelSstPtr p = it.Get();
            TMemIterator c = (resumeLevels && memResumeIt) ? *memResumeIt : TMemIterator(p.SstPtr.Get());
            if (!(resumeLevels && memResumeIt)) {
                c.SeekToFirst();
            }
            // consume the resume state only for the first SST after a resume
            resumeLevels = false;
            memResumeIt.reset();
            while (c.Valid()) {
                aggr->UpdateLevel(p, c.GetCurKey(), c.GetMemRec());
                c.Next();
                if (c.Valid() && yeildChecker.StepAndCheckForYield()) {
                    return TYeildedState{typename TYeildedState::TLevelPosition{it, c}};
                }
            }
            it.Next();
        }

        aggr->Finish();
        return std::nullopt;
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
