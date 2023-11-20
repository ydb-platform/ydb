#pragma once

#include "defs.h"
#include "hullds_sstslice.h"
#include "hullds_idxsnap.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_settings.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TLevelIndexBase
    //////////////////////////////////////////////////////////////////////////////////////////
    class TLevelIndexBase : public TThrRefBase {
    public:
        TLevelIndexBase(const TLevelIndexSettings &settings);

        //////////////////////////////////////////////////////////////////////////////////////
        // Compaction state of TLevelIndex
        //////////////////////////////////////////////////////////////////////////////////////
        enum ELevelCompState {
            StateNoComp = 0,        // default initial state
            StateCompPolicyAtWork,  // compaction policy is working
            StateCompInProgress,    // level compaction
            StateWaitCommit         // wait for commit to disk
        };

        static const char *LevelCompStateToStr(ELevelCompState s);
        ELevelCompState GetCompState() const;
        TString GetCompStateStr(ui32 readsInFlight, ui32 writesInFlight) const;
        void SetCompState(ELevelCompState state);
        static bool CheckEntryPoint(const char *begin, const char *end, size_t keySizeOf, ui32 signature,
                                    TString &explanation);
        static bool CheckBulkFormedSegments(const char *begin, const char *end);
        // Convert OLD data format to new protobuf format
        static void ConvertToProto(NKikimrVDiskData::TLevelIndex &pb,
                                   const char *begin,
                                   const char *end,
                                   size_t keySizeOf);
    public:
        const TLevelIndexSettings Settings;
    private:
        ELevelCompState LevelCompState = StateNoComp;
        TInstant StartTime;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TCompactedLsn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TCompactedLsn {
    public:
        TCompactedLsn(const TCompactedLsn &) = default;
        TCompactedLsn &operator=(const TCompactedLsn &) = default;

        TCompactedLsn() {
            // empty entry point case; this is good, we start an empty db
            ExplicitlySet = true;
            Lsn = TOptLsn::NotSet;
        }

        TCompactedLsn(const NKikimrVDiskData::TLevelIndex &pb) {
            if (pb.HasCompactedLsn()) {
                // good case, conversion has been performed or initially good database
                ExplicitlySet = true;
                Derived = false;
                Lsn = TOptLsn::CreateFromAny(pb.GetCompactedLsn());
            } else {
                // old database, conversion has not been performed, because there was
                // no any fresh compaction yet
                ExplicitlySet = false;
                Derived = false;
                Lsn = TOptLsn::NotSet;
            }
        }

        bool Valid() const {
            return ExplicitlySet || Derived;
        }

        void SerializeToProto(NKikimrVDiskData::TLevelIndex &pb) const {
            Y_ABORT_UNLESS(Valid());
            if (ExplicitlySet) {
                // we save current value iff if was explicitly set
                pb.SetCompactedLsn(Lsn.Value());
            }
        }

        void Update(ui64 lsn) {
            Y_ABORT_UNLESS(lsn > 0 && Valid());
            ExplicitlySet = true;
            Lsn.SetMax(lsn);
        }

        // this method is for backward compatibility support; normally we
        // initialize Lsn from NKikimrVDiskData::TLevelIndex.CompactedLsn,
        // otherwise we fall back to old incorrect method of selecting
        // max lsn in index
        void UpdateWithObsoleteLastCompactedLsn(TOptLsn m) {
            if (ExplicitlySet) {
                // NOTE: normally expression 'm <= Lsn' would be true, but unfortunately
                //       we can't put a VERIFY here, because we saw this real situation:
                //       1. VDisk doesn't have any records in fresh, it starts as an empty db
                //       2. VDisk gets only repl SSTs from other VDisks; they are being put
                //          to Level 0 with REPL flag
                //       3. Later VDisk compacts them from Level 0 to, say, Level 1 and
                //          they gen COMP flag
                //       4. On restart we calculate LastCompactedLsn lsn that would be not 0,
                //          because REPL SSTs had some none 0 lsns
                //       5. Crash! This lsn (m) is larger than Lsn (which is actually not set)
                //
                // Conclusion: only saving LastCompactedLsn explicitly works. Other fork of
                //  code is for migration only
            } else {
                Derived = true;
                Lsn = m;
            }
        }

        TString ToString() const {
            TStringStream str;
            auto b = [] (bool b) { return b ? "true" : "false"; };
            str << "[ExplicitlySet# " << b(ExplicitlySet) << " Derived# " << b(Derived) << " Lsn# " << Lsn << "]";
            return str.Str();
        }

        bool operator < (ui64 lsn) const { Y_ABORT_UNLESS(Valid()); return Lsn < lsn; }
        bool operator <=(ui64 lsn) const { Y_ABORT_UNLESS(Valid()); return Lsn <= lsn; }
        bool operator > (ui64 lsn) const { Y_ABORT_UNLESS(Valid()); return Lsn > lsn; }
        bool operator >=(ui64 lsn) const { Y_ABORT_UNLESS(Valid()); return Lsn >= lsn; }

    private:
        // all log records <= Lsn have been compacted
        TOptLsn Lsn = TOptLsn::NotSet;
        // current value of Lsn was explicitly set by fresh compaction
        bool ExplicitlySet = false;
        // current value of Lsn was derived from level index state
        bool Derived = false;
    };

    inline bool operator <=(ui64 lsn, const TCompactedLsn &cl) {
        return cl >= lsn;
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelIndex
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndex : public TLevelIndexBase {
    public:
        typedef NKikimr::TFreshSegment<TKey, TMemRec> TFreshSegment;
        typedef NKikimr::TFreshData<TKey, TMemRec> TFreshData;
        typedef NKikimr::TFreshDataSnapshot<TKey, TMemRec> TFreshDataSnapshot;
        typedef NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;
        typedef TIntrusivePtr<TLevelSlice> TLevelSlicePtr;
        typedef NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
        typedef NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef NKikimr::TFreshAppendix<TKey, TMemRec> TFreshAppendix;

        static constexpr ui32 GetSignature() {
            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                return 0x5567FEDC;
            } else if constexpr (std::is_same_v<TKey, TKeyBlock>) {
                return 0x5567FEDD;
            } else if constexpr (std::is_same_v<TKey, TKeyBarrier>) {
                return 0x5567FEDE;
            } else {
                static_assert(!std::is_same_v<TKey, TKey>, "invalid TKey");
            }
        }

        static constexpr ui32 Signature = GetSignature();
        static constexpr size_t KeySizeOf = sizeof(TKey);

    private:
        std::shared_ptr<TLevelIndexCtx> Ctx;
        TFreshData Fresh;

    public:
        TLevelSlicePtr CurSlice;
        TList<TIntrusivePtr<TLevelSegment>> UncommittedReplSegments;

        TActorId LIActor;
        ui64 CurEntryPointLsn = ui64(-1);
        ui64 PrevEntryPointLsn = ui64(-1);

        TAtomic FreshCompWritesInFlight = 0;
        TAtomic HullCompReadsInFlight = 0;
        TAtomic HullCompWritesInFlight = 0;

        TIntrusivePtr<TDelayedCompactionDeleterInfo> DelayedCompactionDeleterInfo;
        std::shared_ptr<TLevelIndexActorCtx> ActorCtx;

    private:
        // it is used for allocation unique id to a new sst
        TAtomic NextSstId = 0;
        // Is index loaded into memory
        bool Loaded = false;
        // The compacted data stretches for some diapason of lsns, actually from 0 and up to
        // CompactedLsn. We use CompactedLsn at recovery to determine what logs records
        // have to be ignored
        TCompactedLsn CompactedLsn;
        // Takes snapshot of the database, actorSystem must be provided, if actorSystem is null,
        // optimization applies;
        // This function is private and must not be called directly
        TLevelIndexSnapshot PrivateGetSnapshot(TActorSystem *actorSystemToNotifyLevelIndex) {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            return TLevelIndexSnapshot(CurSlice, Fresh.GetSnapshot(), CurSlice->Level0CurSstsNum(),
                    actorSystemToNotifyLevelIndex, DelayedCompactionDeleterInfo);
        }

    public:
        void Output(IOutputStream &str) const {
            str << "{Db# " << TKey::Name()
                << " CurEntryPointLsn# " << CurEntryPointLsn
                << " PrevEntryPointLsn# " << PrevEntryPointLsn
                << " CompactedLsn# " << CompactedLsn.ToString()
                << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        // Constructors just create corresponding structures in memory from entryPoint record or from nothing;
        // nothing is loaded into memory from disk, use TLevelIndexLoader for bringing index to memory
        TLevelIndex(const TLevelIndexSettings &settings, std::shared_ptr<TRopeArena> arena)
            : TLevelIndexBase(settings)
            , Ctx(std::make_shared<TLevelIndexCtx>())
            , Fresh(settings, TAppData::TimeProvider, std::move(arena))
            , CurSlice(MakeIntrusive<TLevelSlice>(settings, Ctx))
            , DelayedCompactionDeleterInfo(new TDelayedCompactionDeleterInfo)
            , ActorCtx(std::make_shared<TLevelIndexActorCtx>())
            , CompactedLsn()
        {}

        TLevelIndex(const TLevelIndexSettings &settings,
                    const NKikimrVDiskData::TLevelIndex &pb,
                    ui64 entryPointLsn,
                    std::shared_ptr<TRopeArena> arena)
            : TLevelIndexBase(settings)
            , Ctx(std::make_shared<TLevelIndexCtx>())
            , Fresh(settings, TAppData::TimeProvider, std::move(arena))
            , CurSlice(MakeIntrusive<TLevelSlice>(settings, Ctx, pb))
            , CurEntryPointLsn(entryPointLsn)
            , DelayedCompactionDeleterInfo(new TDelayedCompactionDeleterInfo)
            , ActorCtx(std::make_shared<TLevelIndexActorCtx>())
            , NextSstId(pb.GetNextSstId())
            , CompactedLsn(pb)
        {}

        ~TLevelIndex()
        {}

        //////////////////////////////////////////////////////////////////////////////////////
        // Snapshot
        //////////////////////////////////////////////////////////////////////////////////////
        // you can't read from TLevelIndex directly, take a snapshot instead
        TLevelIndexSnapshot GetSnapshot(TActorSystem *as);

        TLevelIndexSnapshot GetIndexSnapshot() {
            return PrivateGetSnapshot(nullptr);
        }

        //////////////////////////////////////////////////////////////////////////////////////
        // Operations with Fresh
        //////////////////////////////////////////////////////////////////////////////////////
        void PutToFresh(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress, TRope buffer) {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            Fresh.PutLogoBlobWithData(lsn, key, partId, ingress, std::move(buffer));
        }

        void PutToFresh(ui64 lsn, const TKey &key, const TMemRec &memRec) {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            Fresh.Put(lsn, key, memRec);
        }

        void PutToFresh(std::shared_ptr<TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn) {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            Fresh.PutAppendix(std::move(a), firstLsn, lastLsn);
        }

        // Fresh Compaction
        bool NeedsFreshCompaction(ui64 yardFreeUpToLsn, bool force) const {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            return Fresh.NeedsCompaction(yardFreeUpToLsn, force);
        }

        TIntrusivePtr<TFreshSegment> FindFreshSegmentForCompaction() {
            return Fresh.FindSegmentForCompaction();
        }

        bool FreshCompactionInProgress() const {
            return Fresh.CompactionInProgress();
        }

        void FreshCompactionFinished() {
            Fresh.CompactionFinished();
        }
        void FreshCompactionSstCreated(TIntrusivePtr<TFreshSegment> &&freshSegment) {
            Fresh.CompactionSstCreated(std::move(freshSegment));
        }

        // Fresh Appendix Compaction
        typename TFreshData::TCompactionJob CompactFreshAppendix() {
            return Fresh.CompactAppendix();
        }
        //////////////////////////////////////////////////////////////////////////////////////

        ui64 GetFirstLsnToKeep() const {
            Y_DEBUG_ABORT_UNLESS(Loaded);
            return Min(Min(CurEntryPointLsn, PrevEntryPointLsn), Fresh.GetFirstLsnToKeep());
        }

        virtual void LoadCompleted() {
            Y_DEBUG_ABORT_UNLESS(!Loaded);
            Loaded = true;

            // NOTE: compatibility issue, see comment for UpdateWithObsoleteLastCompactedLsn method
            TOptLsn m = CurSlice->ObsoleteLastCompactedLsn();
            CompactedLsn.UpdateWithObsoleteLastCompactedLsn(m);
        }

        // Skip record? This method is used when recovering, those log records, that were already
        // compacted must be skipped
        bool SkipRecord(ui64 lsn) const {
            return lsn <= CompactedLsn;
        }

        const TCompactedLsn &GetCompactedLsn() const {
            return CompactedLsn;
        }

        void UpdateCompactedLsn(ui64 lsn) {
            CompactedLsn.Update(lsn);
        }

        ui64 AllocSstId() {
            return AtomicIncrement(NextSstId);
        }

        bool Empty() const {
            return Fresh.Empty() && CurSlice->Empty();
        }

        bool FullyCompacted() const {
            return Fresh.Empty() && CurSlice->FullyCompacted();
        }

        bool IsWrittenToSstBeforeLsn(ui64 lsn) const {
            return Fresh.Empty() || (Fresh.GetFirstLsn() > lsn);
        }

        TSatisfactionRank GetSatisfactionRank(ESatisfactionRankType s) const {
            switch (s) {
                case ESatisfactionRankType::Fresh: return Fresh.GetSatisfactionRank();
                case ESatisfactionRankType::Level: return CurSlice->GetSatisfactionRank();
                default: Y_ABORT("Unexpected rank type");
            }
        }

        void OutputHtml(const TString &name, IOutputStream &str) const;
        void OutputProto(NKikimrVDisk::LevelIndexStat *stat) const;
        void OutputHugeStatButton(const TString &name, IOutputStream &str) const;
        void OutputQueryDbButton(const TString &name, IOutputStream &str) const;

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            // include fresh
            Fresh.GetOwnedChunks(chunks);
            // include slice
            CurSlice->GetOwnedChunks(chunks);
        }

        void SerializeToProto(NKikimrVDiskData::TLevelIndex &pb) const {
            static_assert(sizeof(Signature) == sizeof(ui32),
                          "incorrect signature size");
            static_assert(sizeof(NextSstId) == sizeof(ui64),
                          "incorrect size of NextSstId field: must be 64 bits wide");

            // write next SST index
            ui64 nextSstId = AtomicGet(NextSstId);
            pb.SetNextSstId(nextSstId);
            CompactedLsn.SerializeToProto(pb);
            CurSlice->SerializeToProto(pb);

            // add yet uncommitted SSTables into entrypoint
            auto& pbLevel0 = *pb.MutableLevel0();
            auto& bulkFormedSstInfoSet = *pb.MutableBulkFormedSstInfoSet();
            for (const TIntrusivePtr<TLevelSegment>& seg : UncommittedReplSegments) {
                Y_ABORT_UNLESS(seg->Info.FirstLsn && seg->Info.LastLsn);
                // store level-0 SSTable
                seg->SerializeToProto(*pbLevel0.AddSsts());

                // store bulk-formed segment information
                auto& bulkSeg = *bulkFormedSstInfoSet.AddSegments();
                bulkSeg.SetFirstBlobLsn(seg->Info.FirstLsn);
                bulkSeg.SetLastBlobLsn(seg->Info.LastLsn);
                seg->GetEntryPoint().SerializeToProto(*bulkSeg.MutableEntryPoint());
            }

        }

        void ApplyUncommittedReplSegment(TIntrusivePtr<TLevelSegment>&& seg, const THullCtxPtr &hullCtx) {
            // remove this SST from uncommitted list
            auto it = std::find(UncommittedReplSegments.begin(), UncommittedReplSegments.end(), seg);
            Y_ABORT_UNLESS(it != UncommittedReplSegments.end());
            UncommittedReplSegments.erase(it);

            // create matching entry in bulk-formed segments list
            CurSlice->BulkFormedSegments.AddBulkFormedSst(seg->Info.FirstLsn, seg->Info.LastLsn, seg->GetEntryPoint());

            // put SST to level 0
            InsertSstAtLevel0(seg, hullCtx);
        }

        void InsertSstAtLevel0(const TIntrusivePtr<TLevelSegment>& seg, const THullCtxPtr &hullCtx) {
            CurSlice->Level0.Put(seg);

            // update storage ratio info
            if (auto ratio = seg->StorageRatio.Get()) {
                CurSlice->LastPublishedRatio += *ratio;
                hullCtx->UpdateSpaceCounters(NHullComp::TSstRatio(), *ratio);
            }
        }

        // update statistics about levels
        void UpdateLevelStat(NMonGroup::TLsmAllLevelsStat &stat);
    };

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Custom implementation of GetSnapshot for every Hull Database (optimization applies)
    ///////////////////////////////////////////////////////////////////////////////////////////
    template <>
    inline TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob> TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>::GetSnapshot(TActorSystem *as) {
        return PrivateGetSnapshot(as);
    }

    template <>
    inline TLevelIndexSnapshot<TKeyBlock, TMemRecBlock> TLevelIndex<TKeyBlock, TMemRecBlock>::GetSnapshot(TActorSystem *) {
        // we never want to take data snapshots for Blocks Database
        return PrivateGetSnapshot(nullptr);
    }

    template <>
    inline TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier> TLevelIndex<TKeyBarrier, TMemRecBarrier>::GetSnapshot(TActorSystem *) {
        // we never want to take data snapshots for Barriers Database
        return PrivateGetSnapshot(nullptr);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Custom implementation of UpdateLevelStat
    ///////////////////////////////////////////////////////////////////////////////////////////
    template <>
    inline void TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>::UpdateLevelStat(NMonGroup::TLsmAllLevelsStat &stat) {
        struct TLevelGroupInfo {
            ui64 SstNum = 0;
            ui64 NumItems = 0;
            ui64 NumItemsInplaced = 0;
            ui64 NumItemsHuge = 0;
            ui64 DataInplaced = 0;
            ui64 DataHuge = 0;
        };

        TLevelGroupInfo level0, level1to8, level9to16, level17, level18;

        auto process = [](TLevelGroupInfo *stat, TLevelSegment *seg) {
            stat->SstNum += 1;
            stat->NumItems += seg->Info.Items;
            stat->NumItemsInplaced += seg->Info.ItemsWithInplacedData;
            stat->NumItemsHuge += seg->Info.ItemsWithHugeData;
            stat->DataInplaced += seg->Info.InplaceDataTotalSize;
            stat->DataHuge += seg->Info.HugeDataTotalSize;
        };

        for (const auto& seg : CurSlice->Level0.Segs->Segments) {
            process(&level0, seg.Get());
        }

        ui32 levelIndex = 1;
        for (const auto& level : CurSlice->SortedLevels) {
            TLevelGroupInfo *info = nullptr;
            switch (levelIndex) {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                case 8:
                    info = &level1to8;
                    break;

                case 9:
                case 10:
                case 11:
                case 12:
                case 13:
                case 14:
                case 15:
                case 16:
                    info = &level9to16;
                    break;

                case 17:
                    info = &level17;
                    break;

                case 18:
                    info = &level18;
                    break;
            }

            if (info) {
                for (const auto& seg : level.Segs->Segments) {
                    process(info, seg.Get());
                }
            }

            ++levelIndex;
        }

        for (const auto& p : {std::make_pair(&level0, &stat.Level0),
                std::make_pair(&level1to8, &stat.Level1to8),
                std::make_pair(&level9to16, &stat.Level9to16),
                std::make_pair(&level17, &stat.Level17),
                std::make_pair(&level18, &stat.Level18)}) {
            TLevelGroupInfo *from = p.first;
            NMonGroup::TLsmLevelGroup *to = p.second;
            to->SstNum() = from->SstNum;
            to->NumItems() = from->NumItems;
            to->NumItemsInplaced() = from->NumItemsInplaced;
            to->NumItemsHuge() = from->NumItemsHuge;
            to->DataInplaced() = from->DataInplaced;
            to->DataHuge() = from->DataHuge;
        }
    }

    template <>
    inline void TLevelIndex<TKeyBlock, TMemRecBlock>::UpdateLevelStat(NMonGroup::TLsmAllLevelsStat &) {
        // intentionally empty
    }

    template <>
    inline void TLevelIndex<TKeyBarrier, TMemRecBarrier>::UpdateLevelStat(NMonGroup::TLsmAllLevelsStat &) {
        // intentionally empty
    }

    template <class TKey, class TMemRec>
    inline void TLevelIndex<TKey, TMemRec>::OutputHugeStatButton(const TString& /*name*/, IOutputStream& /*str*/) const {
        // by default don't output HugeStat button
    }

    template <>
    inline void TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>::OutputHugeStatButton(const TString& /*name*/, IOutputStream &str) const
    {
        str << "<a class=\"btn btn-primary btn-xs navbar-right\""
            << " href=\"?type=hugestat\">Huge Stat</a>";
    }

    template <class TKey, class TMemRec>
    inline void TLevelIndex<TKey, TMemRec>::OutputQueryDbButton(const TString& /*name*/, IOutputStream& /*str*/) const {
        // by default don't output QueryDb button
    }

    template <>
    inline void TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>::OutputQueryDbButton(const TString &name, IOutputStream &str) const
    {
        str << "<a class=\"btn btn-primary btn-xs navbar-right\""
            << " href=\"?type=query&dbname=" << name << "&form=1\">Query Database</a>";
    }

    template <>
    inline void TLevelIndex<TKeyBarrier, TMemRecBarrier>::OutputQueryDbButton(const TString &name, IOutputStream &str) const
    {
        str << "<a class=\"btn btn-primary btn-xs navbar-right\""
            << " href=\"?type=query&dbname=" << name << "&form=1\">Query Database</a>";
    }

    extern template class TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TLevelIndex<TKeyBarrier, TMemRecBarrier>;
    extern template class TLevelIndex<TKeyBlock, TMemRecBlock>;

} // NKikimr

