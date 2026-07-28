#pragma once

#include "defs.h"
#include "query_stat_yield.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

#include <util/stream/length.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TraverseFreshSegment
    // Traverses a single fresh segment. May yield mid-traversal and resume
    // from the yielded position later.
    //
    ////////////////////////////////////////////////////////////////////////////
    template <class TAggr, class TKey, class TMemRec>
    std::optional<TDbStatYieldedState<TKey, TMemRec>> TraverseFreshSegment(
            const TIntrusivePtr<THullCtx>& hullCtx,
            TAggr* aggr,
            const char* segmentName,
            const ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>& segment,
            typename TDbStatYieldedState<TKey, TMemRec>::EFreshSegment segmentType,
            const std::optional<TKey>& resumeKey,
            TDbStatYieldChecker& yieldChecker)
    {
        using TYieldedState = TDbStatYieldedState<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TFreshIterator = typename TFreshSegmentSnapshot::TIteratorWOMerge;

        TFreshIterator freshIterator(hullCtx, &segment);
        if (resumeKey) {
            freshIterator.Seek(*resumeKey);
        } else {
            freshIterator.SeekToFirst();
        }

        while (freshIterator.Valid()) {
            aggr->UpdateFresh(segmentName, freshIterator.GetUnmergedKey(), freshIterator.GetUnmergedMemRec());
            freshIterator.Next();
            if (freshIterator.Valid() && yieldChecker.StepAndCheckForYield()) {
                return TYieldedState{typename TYieldedState::TFreshPosition{
                    segmentType, freshIterator.GetUnmergedKey()}};
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
    std::optional<TDbStatYieldedState<TKey, TMemRec>> TraverseDbWithoutMerge(
            const TIntrusivePtr<THullCtx>& hullCtx,
            TAggr* aggr,
            const ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>& snap,
            std::optional<TDbStatYieldedState<TKey, TMemRec>> yieldedState = std::nullopt,
            std::optional<TDbStatYieldPolicy> yieldPolicy = std::nullopt,
            TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
    {
        using TYieldedState = TDbStatYieldedState<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TMemIterator = typename TLevelSegment::TMemIterator;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
        using EFreshSegment = typename TYieldedState::EFreshSegment;
        using TLevelPosition = typename TYieldedState::TLevelPosition;
        using TUnsortedLevelDiscriminator = typename TLevelPosition::TUnsortedLevelDiscriminator;
        using TSortedLevelDiscriminator = typename TLevelPosition::TSortedLevelDiscriminator;

        TDbStatYieldChecker yieldChecker(std::move(yieldPolicy), std::move(monotonicTimeProvider));

        // Description of a single fresh segment to traverse
        struct TSegmentDescription {
            const char* Name;
            EFreshSegment Type;
            const TFreshSegmentSnapshot& Segment;
        };
        const TSegmentDescription segments[] = {
            {"FCur", EFreshSegment::Cur, snap.FreshSnap.Cur},
            {"FDreg", EFreshSegment::Dreg, snap.FreshSnap.Dreg},
            {"FOld", EFreshSegment::Old, snap.FreshSnap.Old},
        };

        // Figure out where to (re)start traversal
        size_t startFreshSegmentIdx = 0;
        bool resumeLevels = false;
        std::optional<TKey> freshResumeKey;
        std::optional<TLevelPosition> levelResumePosition;

        if (yieldedState) {
            using TFreshPosition = typename TYieldedState::TFreshPosition;
            if (TFreshPosition* freshPosition = std::get_if<TFreshPosition>(&yieldedState->Position)) {
                startFreshSegmentIdx = static_cast<size_t>(freshPosition->Segment);
                freshResumeKey = freshPosition->Key;
            } else {
                levelResumePosition = std::get<TLevelPosition>(yieldedState->Position);
                resumeLevels = true;
            }
        }

        auto sstSortsBeforeSavedPosition = [](const TLevelSstPtr& levelSstPtr,
                const TLevelPosition& savedPosition) -> bool {
            if (levelSstPtr.Level != savedPosition.Level) {
                return levelSstPtr.Level < savedPosition.Level;
            }
            if (levelSstPtr.Level == 0) {
                return levelSstPtr.SstPtr->VolatileOrderId <
                    std::get<TUnsortedLevelDiscriminator>(savedPosition.Discriminator);
            }
            return levelSstPtr.SstPtr->FirstKey() <
                std::get<TSortedLevelDiscriminator>(savedPosition.Discriminator);
        };
        auto sstMatchesSavedPosition = [](const TLevelSstPtr& levelSstPtr,
                const TLevelPosition& savedPosition) -> bool {
            if (levelSstPtr.Level != savedPosition.Level) {
                return false;
            }
            if (levelSstPtr.Level == 0) {
                return levelSstPtr.SstPtr->VolatileOrderId ==
                    std::get<TUnsortedLevelDiscriminator>(savedPosition.Discriminator);
            }
            const TKey& firstKey = levelSstPtr.SstPtr->FirstKey();
            const TKey& savedFirstKey = std::get<TSortedLevelDiscriminator>(savedPosition.Discriminator);
            return !(firstKey < savedFirstKey) && !(savedFirstKey < firstKey);
        };
        auto makeLevelPosition = [](const TLevelSstPtr& levelSstPtr, const TKey& nextKey) -> TLevelPosition {
            TLevelPosition position;
            position.Level = levelSstPtr.Level;
            position.Key = nextKey;
            if (levelSstPtr.Level == 0) {
                position.Discriminator = TUnsortedLevelDiscriminator(levelSstPtr.SstPtr->VolatileOrderId);
            } else {
                position.Discriminator = TSortedLevelDiscriminator(levelSstPtr.SstPtr->FirstKey());
            }
            return position;
        };

        // Traverse Fresh
        if (!resumeLevels) {
            for (size_t segmentIdx = startFreshSegmentIdx; segmentIdx < std::size(segments); ++segmentIdx) {
                const TSegmentDescription &description = segments[segmentIdx];
                std::optional<TKey> resumeKey;
                if (segmentIdx == startFreshSegmentIdx) {
                    resumeKey = freshResumeKey;
                }
                if (std::optional<TYieldedState> yielded = TraverseFreshSegment(hullCtx, aggr, description.Name,
                        description.Segment, description.Type, resumeKey, yieldChecker)) {
                    return yielded;
                }
            }
        }

        // Traverse SSTs
        TSstIterator sstIterator(&snap.SliceSnap);
        sstIterator.SeekToFirst();

        // When resuming the level phase, skip SSTs that are ordered strictly before the
        // saved position.
        if (resumeLevels) {
            while (sstIterator.Valid() &&
                    sstSortsBeforeSavedPosition(sstIterator.Get(), *levelResumePosition)) {
                sstIterator.Next();
            }
        }

        while (sstIterator.Valid()) {
            TLevelSstPtr levelSstPtr = sstIterator.Get();
            TMemIterator memIterator(levelSstPtr.SstPtr.Get());
            if (resumeLevels && sstMatchesSavedPosition(levelSstPtr, *levelResumePosition)) {
                memIterator.Seek(levelResumePosition->Key);
            } else {
                memIterator.SeekToFirst();
            }
            // resume applies only to the first matching SST
            resumeLevels = false;
            while (memIterator.Valid()) {
                aggr->UpdateLevel(levelSstPtr, memIterator.GetCurKey(), memIterator.GetMemRec());
                memIterator.Next();
                if (memIterator.Valid() && yieldChecker.StepAndCheckForYield()) {
                    return TYieldedState{makeLevelPosition(levelSstPtr, memIterator.GetCurKey())};
                }
            }
            sstIterator.Next();
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
