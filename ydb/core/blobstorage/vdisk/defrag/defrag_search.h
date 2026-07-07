#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_defrag.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullstorageratio.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>
#include <ydb/core/blobstorage/vdisk/query/query_statalgo.h>

namespace NKikimr {

    namespace NDefrag {
        static constexpr TDuration MaxSnapshotHoldDuration = TDuration::Seconds(30);
        static constexpr TDuration WorkQuantum = TDuration::MilliSeconds(10);
    }

    struct TChunksToDefrag {
        TDefragChunks Chunks;
        ui32 FoundChunksToDefrag = 0;
        ui64 EstimatedSlotsCount = 0;
        bool IsShred = false;
        THashSet<TChunkIdx> ChunksToShred;

        static TChunksToDefrag Shred(THashSet<TChunkIdx> chunksToShred) {
            return {
                .IsShred = true,
                .ChunksToShred = std::move(chunksToShred),
            };
        }

        void Output(IOutputStream &str) const {
            str << "{Chunks# " << FormatList(Chunks);
            str << " EstimatedSlotsCount# " << EstimatedSlotsCount << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        explicit operator bool() const {
            return !Chunks.empty();
        }
    };

    struct TDefragRecord {
        // Record key, PartId is not 0
        TLogoBlobID LogoBlobId;
        // old huge blob address to read and rewrite
        TDiskPart OldDiskPart;

        TDefragRecord() = default;
        TDefragRecord(const TLogoBlobID &id, const TDiskPart &part)
            : LogoBlobId(id)
            , OldDiskPart(part)
        {}
    };

    template<typename TDerived>
    class TDefragScanner {
    protected:
        using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;

    private:
        THullDsSnap FullSnap;
    protected:
        // Widened to protected so a combined collector can reuse the barriers essence / GType / keep-flags
        // to compute per-SST storage ratio during the same walk (declaration order preserved for init).
        const TBlobStorageGroupType GType;
        const TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Barriers;
        const bool AllowKeepFlags;
    private:
        TLogoBlobsSnapshot::TForwardIterator Iter;

        TKeyLogoBlob Key;
        std::optional<TMemRecLogoBlob> MemRec;
        NMatrix::TVectorType SeenParts;
        std::array<std::tuple<ui64, TDiskPart, const TLevelSegment*>, 8> PartInfo;

    public:
        TDefragScanner(THullDsSnap&& fullSnap, std::optional<TKeyLogoBlob> seek = std::nullopt)
            : FullSnap(std::move(fullSnap))
            , GType(FullSnap.HullCtx->VCtx->Top->GType)
            , Barriers(FullSnap.BarriersSnap.CreateEssence(FullSnap.HullCtx))
            , AllowKeepFlags(FullSnap.HullCtx->AllowKeepFlags)
            , Iter(FullSnap.HullCtx, &FullSnap.LogoBlobsSnap)
            , SeenParts(0, GType.TotalPartCount())
        {
            if (seek) {
                Iter.Seek(*seek);
            } else {
                Iter.SeekToFirst();
            }
            FullSnap.BarriersSnap.Destroy();
            FullSnap.BlocksSnap.Destroy();
        }

        std::optional<TKeyLogoBlob> Scan(TDuration maxTime) {
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt(&Iter);
            ui64 endTime = GetCycleCountFast() + DurationToCycles(maxTime);
            ui32 count = 0;
            auto callback = [&](TKeyLogoBlob /*key*/, auto* /*merger*/) {
                return ++count % 1024 != 0 || GetCycleCountFast() < endTime;
            };
            heapIt.Walk(std::nullopt, this, callback);
            return heapIt.Valid()
                ? std::make_optional(heapIt.GetCurKey())
                : std::nullopt;
        }

        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* /*data*/, const TKeyLogoBlob& key, ui64 lsn) {
            static_cast<TDerived&>(*this).OnAddFromFresh(memRec, key, lsn);
            Update(memRec, nullptr, key, lsn, nullptr);
        }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key, ui64 circaLsn,
                const TLevelSegment *sst) {
            static_cast<TDerived&>(*this).OnAddFromSegment(memRec, outbound, key, sst);
            Update(memRec, outbound, key, circaLsn, sst);
        }

        static constexpr bool HaveToMergeData() { return false; }

        void Clear() {
            static_cast<TDerived&>(*this).OnClear();
            Key = TKeyLogoBlob();
            MemRec.reset();
            SeenParts.Clear();
        }

        void Finish() {
            Y_DEBUG_ABORT_UNLESS(Key != TKeyLogoBlob());
            Y_DEBUG_ABORT_UNLESS(MemRec);
            const auto status = Barriers->Keep(Key, *MemRec, {}, AllowKeepFlags, true /*allowGarbageCollection*/);
            for (ui8 partIdx : SeenParts) {
                const auto& [lsn, part, sst] = PartInfo[partIdx];
                if (!part.Empty()) {
                    static_cast<TDerived&>(*this).Add(part, TLogoBlobID(Key.LogoBlobID(), partIdx + 1), status.KeepData,
                        sst);
                }
            }
            static_cast<TDerived&>(*this).OnFinish(Key);
        }

    protected:
        // Default no-op CRTP hooks; a derived collector overrides them to accumulate extra per-source /
        // per-SST statistics (e.g. storage ratio) during the same walk. Existing derived scanners inherit
        // these no-ops and are unaffected.
        void OnAddFromSegment(const TMemRecLogoBlob&, const TDiskPart*, const TKeyLogoBlob&, const TLevelSegment*) {}
        void OnAddFromFresh(const TMemRecLogoBlob&, const TKeyLogoBlob&, ui64) {}
        void OnFinish(const TKeyLogoBlob&) {}
        void OnClear() {}

    private:
        void Update(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key, ui64 lsn,
                const TLevelSegment *sst) {
            Y_DEBUG_ABORT_UNLESS(Key == TKeyLogoBlob() || Key == key);
            Key = key;
            if (MemRec) {
                MemRec->Merge(memRec, key, false, GType);
            } else {
                MemRec.emplace(memRec).SetNoBlob();
            }

            if (memRec.GetType() != TBlobType::HugeBlob && memRec.GetType() != TBlobType::ManyHugeBlobs) {
                return;
            }

            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);

            const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
            Y_DEBUG_ABORT_UNLESS(local.CountBits() == extr.End - extr.Begin);

            const TDiskPart *p = extr.Begin;
            for (ui8 partIdx : local) {
                const TDiskPart& part = *p++;
                auto& item = PartInfo[partIdx];
                std::optional<TDiskPart> obsolete;
                if (!SeenParts.Get(partIdx) || std::get<0>(item) < lsn) {
                    obsolete = SeenParts.Get(partIdx) ? std::make_optional(std::get<1>(item)) : std::nullopt;
                    item = {lsn, part, sst};
                    SeenParts.Set(partIdx);
                } else {
                    obsolete.emplace(part);
                }
                if (obsolete && !obsolete->Empty()) {
                    static_cast<TDerived&>(*this).Add(*obsolete, TLogoBlobID(key.LogoBlobID(), partIdx + 1), false, sst);
                }
            }
            Y_DEBUG_ABORT_UNLESS(p == extr.End);
        }
    };

    class TDefragQuantumChunkFinder {
    private:
        // Info gathered per chunk
        struct TChunkInfo {
            ui32 UsefulSlots = 0; // slots which are used by blobs within the barrier and not obselete
            ui32 OccupiedSlots = 0; // slots which are used by blobs within and behind the barrier or by obsolete blobs
            const ui32 SlotSize;
            const ui32 NumberOfSlotsInChunk;

            TChunkInfo(ui32 slotSize, ui32 numberOfSlotsInChunk)
                : SlotSize(slotSize)
                , NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}

            TString ToString() const {
                TStringStream str;
                str << "UsefulSlots# " << UsefulSlots << " / OccupiedSlots# " << OccupiedSlots << " / " << NumberOfSlotsInChunk;
                return str.Str();
            }
        };

        // Aggregated info gathered per slotSize
        struct TAggrSlotInfo {
            ui32 UsefulSlots = 0;
            ui32 OccupiedSlots = 0;
            ui32 UsedChunks = 0;
            const ui32 NumberOfSlotsInChunk;

            TAggrSlotInfo(ui32 numberOfSlotsInChunk)
                : NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}
        };

        class TChunksMap {
        private:
            using TPerChunkMap = THashMap<ui32, TChunkInfo>; // chunkIdx -> TChunkInfo
            using TAggrBySlotSize = THashMap<ui32, TAggrSlotInfo>; // slotSize -> TAggrSlotInfo
            const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
            TPerChunkMap PerChunkMap;

        private:
            TAggrBySlotSize AggregatePerSlotSize() const {
                TAggrBySlotSize aggrSlots;
                for (const auto& [chunkIdx, chunk] : PerChunkMap) {
                    auto it = aggrSlots.try_emplace(chunk.SlotSize, chunk.NumberOfSlotsInChunk).first;
                    TAggrSlotInfo& aggr = it->second;
                    if (chunk.UsefulSlots > 0) { // we don't want to count chunks with no useful slots (they are already free and will be freed via compaction anyway)
                        aggr.UsefulSlots += chunk.UsefulSlots;
                        aggr.OccupiedSlots += chunk.OccupiedSlots;
                        ++aggr.UsedChunks;
                    }
                }
                return aggrSlots;
            }

        public:
            TChunksMap(const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx)
                : HugeBlobCtx(hugeBlobCtx)
            {}

            void Add(TDiskPart part, const TLogoBlobID& /*id*/, bool useful) {
                auto it = PerChunkMap.find(part.ChunkIdx);
                if (it == PerChunkMap.end()) {
                    const THugeSlotsMap::TSlotInfo *slotInfo = HugeBlobCtx->HugeSlotsMap->GetSlotInfo(part.Size);
                    Y_ABORT_UNLESS(slotInfo, "size# %" PRIu32, part.Size);
                    it = PerChunkMap.emplace(std::piecewise_construct, std::make_tuple(part.ChunkIdx),
                        std::make_tuple(slotInfo->SlotSize, slotInfo->NumberOfSlotsInChunk)).first;
                }
                it->second.UsefulSlots += useful;
                it->second.OccupiedSlots++;
            }

            TChunksToDefrag GetChunksToDefrag(size_t maxChunksToDefrag) const {
                TAggrBySlotSize aggrSlots = AggregatePerSlotSize();

                std::vector<const TPerChunkMap::value_type*> chunks;
                chunks.reserve(PerChunkMap.size());
                for (const auto& kv : PerChunkMap) {
                    chunks.push_back(&kv);
                }
                auto cmpByMoveSize = [](const auto *left, const auto *right) {
                    return left->second.UsefulSlots < right->second.UsefulSlots;
                };
                std::sort(chunks.begin(), chunks.end(), cmpByMoveSize);

                TChunksToDefrag result;
                result.Chunks.reserve(maxChunksToDefrag);

                for (const auto *kv : chunks) {
                    const auto& [chunkIdx, chunk] = *kv;
                    if (chunk.UsefulSlots == 0) {
                        continue; // we don't want to defrag chunks with no useful slots (they are already "free" and will be freed via next compaction anyway)
                    }
                    auto it = aggrSlots.find(chunk.SlotSize);
                    Y_ABORT_UNLESS(it != aggrSlots.end());
                    auto& a = it->second;

                    // if we can put all current used slots into UsedChunks - 1, then defragment this chunk
                    if (a.NumberOfSlotsInChunk * (a.UsedChunks - 1) >= a.OccupiedSlots) {
                        --a.UsedChunks;
                        a.OccupiedSlots -= (chunk.OccupiedSlots - chunk.UsefulSlots); // we won't copy "useless" slots to another chunk
                        ++result.FoundChunksToDefrag;
                        if (result.Chunks.size() < maxChunksToDefrag) {
                            result.Chunks.emplace_back(chunkIdx, chunk.SlotSize);
                            result.EstimatedSlotsCount += chunk.UsefulSlots;
                        } else {
                            break;
                        }
                    }
                }

                return result;
            }

            ui64 GetTotalSpaceCouldBeFreedViaCompaction() const {
                ui64 totalSpaceCouldBeFreed = 0;
                for (const auto& [chunkIdx, chunk] : PerChunkMap) {
                    if (chunk.UsefulSlots == 0) {
                        // this chunk has no useful slots, so we can free all its slots
                        totalSpaceCouldBeFreed += chunk.NumberOfSlotsInChunk * chunk.SlotSize;
                    } else {
                        // this chunk has some obsolete or behind the barrier slots, so we can free them
                        ui32 uselessSlots = chunk.OccupiedSlots - chunk.UsefulSlots;
                        totalSpaceCouldBeFreed += uselessSlots * chunk.SlotSize;
                    }
                }
                return totalSpaceCouldBeFreed;
            }

            ui64 GetFreedChunks() const {
                ui64 res = 0;
                for (const auto& [_, chunk] : PerChunkMap) {
                    if (chunk.UsefulSlots == 0) {
                        ++res; // this chunk has no useful slots, so it can be freed
                    }
                }
                return res;
            }

            void Output(IOutputStream &str) const {
                str << "{ChunksMap# [";
                bool first = true;
                for (auto& [chunkId, info] : PerChunkMap) {
                    if (first) {
                        first = false;
                    } else {
                        str << " ";
                    }
                    str << "{chunkId# " << chunkId << " " << info.ToString() << "}";
                }
                str << "]}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        TChunksMap ChunksMap;

    public:
        TDefragQuantumChunkFinder(const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx)
            : ChunksMap(hugeBlobCtx)
        {}

        TChunksToDefrag GetChunksToDefrag(size_t maxChunksToDefrag) {
            return ChunksMap.GetChunksToDefrag(maxChunksToDefrag);
        }

        ui64 GetTotalSpaceCouldBeFreedViaCompaction() const {
            return ChunksMap.GetTotalSpaceCouldBeFreedViaCompaction();
        }

        ui64 GetFreedChunks() const {
            return ChunksMap.GetFreedChunks();
        }

        void Add(TDiskPart part, const TLogoBlobID& id, bool useful, const void* /*sst*/) {
            ChunksMap.Add(part, id, useful);
        }
    };

    class TDefragQuantumFindChunks
        : public TDefragQuantumChunkFinder
        , public TDefragScanner<TDefragQuantumFindChunks>
    {
    public:
        TDefragQuantumFindChunks(THullDsSnap&& snap, const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx)
            : TDefragQuantumChunkFinder(hugeBlobCtx)
            , TDefragScanner(std::move(snap))
        {}

        using TDefragQuantumChunkFinder::Add;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TDefragQuantumFindRecords {
        THashSet<TChunkIdx> Chunks; // chunks to defrag (i.e. move all data from these chunks)
        THashSet<TChunkIdx> LockedChunks; // allow rewrites from
        std::vector<TDefragRecord> RecsToRewrite;
        std::optional<TKeyLogoBlob> NextId;
        THashSet<ui64> TablesToCompact;
        bool NeedsFreshCompaction = false;

        class TScanQuantum
            : public TDefragScanner<TScanQuantum>
        {
            TDefragQuantumFindRecords& Parent;

        public:
            TScanQuantum(THullDsSnap&& fullSnap, std::optional<TKeyLogoBlob> seek, TDefragQuantumFindRecords& parent)
                : TDefragScanner(std::move(fullSnap), seek)
                , Parent(parent)
            {}

            using TDefragScanner::Scan;

            void Add(TDiskPart part, const TLogoBlobID& id, bool useful, const TLevelSegment *sst) {
                if (Parent.Chunks.contains(part.ChunkIdx)) {
                    if (useful && Parent.LockedChunks.contains(part.ChunkIdx)) {
                        Parent.RecsToRewrite.emplace_back(id, part);
                    }
                    if (sst) {
                        Parent.TablesToCompact.insert(sst->AssignedSstId);
                    } else {
                        Parent.NeedsFreshCompaction = true;
                    }
                }
            }
        };

    public:
        TDefragQuantumFindRecords(TChunksToDefrag&& chunksToDefrag, const TDefragChunks& locked) {
            if (chunksToDefrag.IsShred) {
                LockedChunks = Chunks = std::move(chunksToDefrag.ChunksToShred);
            } else {
                for (const auto& chunk : chunksToDefrag.Chunks) {
                    Chunks.insert(chunk.ChunkId);
                }
                for (const auto& chunk : locked) {
                    LockedChunks.insert(chunk.ChunkId);
                }
                Y_ABORT_UNLESS(Chunks.size() == chunksToDefrag.Chunks.size()); // ensure there are no duplicate numbers
            }
        }

        bool Scan(TDuration quota, THullDsSnap fullSnap) {
            NextId = TScanQuantum(std::move(fullSnap), NextId, *this).Scan(quota);
            return NextId.has_value();
        }

        void StartFindingTablesToCompact() {
            RecsToRewrite.clear();
            NextId.reset();
            TablesToCompact.clear();
            NeedsFreshCompaction = false;
        }

        void SetLockedChunks(THashSet<ui32> lockedChunks) {
            LockedChunks = std::move(lockedChunks);
        }

        std::vector<TDefragRecord> GetRecordsToRewrite() { return std::move(RecsToRewrite); }
        THashSet<ui64> GetTablesToCompact() { return std::move(TablesToCompact); }
        bool GetNeedsFreshCompaction() const { return NeedsFreshCompaction; }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // TDefragCalcStatWithRatio
    // Like TDefragCalcStat (per-huge-chunk occupancy for the defrag planner), but ALSO computes per-SST
    // storage ratio (NHullComp::TSstRatio) during the SAME single walk, so one pass serves both the defrag
    // planner and the compaction selector. Ratio semantics mirror NHullComp::TStrategyStorageRatio::
    // CalculateSstRatio() exactly (whole-DB merged keep-flags vs per-SST subset keep-flags).
    ////////////////////////////////////////////////////////////////////////////
    class TDefragCalcStatWithRatio
        : public TDefragScanner<TDefragCalcStatWithRatio>
        , public TDefragQuantumChunkFinder
    {
        // --- per-huge-chunk occupancy (identical to TDefragCalcStat) ---
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        std::unordered_set<ui32> Chunks;
        std::unordered_map<ui32, ui32> Map; // numberOfSlotsInChunk -> usefulSlots

        // --- per-SST storage ratio (mirrors NHullComp::TSinglePassRatioCollector) ---
        struct TPerSstCur {
            const TLevelSegment* Sst = nullptr;
            ui32 NumKeepFlags = 0;
            ui32 NumDoNotKeepFlags = 0;
            ui64 InplacedDataSize = 0;
            ui64 HugeDataSize = 0;
        };
        const bool ProduceRatios;
        TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob> WholeMerger;
        TStackVec<TPerSstCur, 32> CurSsts;
        TKeyLogoBlob RatioKey;
        THashMap<const TLevelSegment*, NHullComp::TSstRatioPtr> Ratios;

    public:
        // produceRatios == false makes this behave exactly like the plain per-chunk defrag stat (the ratio
        // CRTP hooks become no-ops), so the defrag planner pays nothing extra unless the shared-pass control
        // is on.
        TDefragCalcStatWithRatio(THullDsSnap&& fullSnap, const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx,
                bool produceRatios = false)
            : TDefragScanner(std::move(fullSnap))
            , TDefragQuantumChunkFinder(hugeBlobCtx)
            , HugeBlobCtx(hugeBlobCtx)
            , ProduceRatios(produceRatios)
            , WholeMerger(GType)
        {}

        // ---- per-huge-chunk occupancy (identical to TDefragCalcStat) ----
        void Add(TDiskPart part, const TLogoBlobID& id, bool useful, const TLevelSegment *sst) {
            Chunks.insert(part.ChunkIdx);
            if (useful) {
                const THugeSlotsMap::TSlotInfo *slotInfo = HugeBlobCtx->HugeSlotsMap->GetSlotInfo(part.Size);
                Y_ABORT_UNLESS(slotInfo, "size# %" PRIu32, part.Size);
                ++Map[slotInfo->NumberOfSlotsInChunk];
            }
            TDefragQuantumChunkFinder::Add(part, id, useful, sst);
        }

        ui32 GetTotalChunks() {
            return Chunks.size();
        }

        ui32 GetUsefulChunks() {
            ui32 res = 0;
            for (const auto& [numberOfSlotsInChunk, usefulSlots] : Map) {
                res += (usefulSlots + numberOfSlotsInChunk - 1) / numberOfSlotsInChunk;
            }
            return res;
        }

        // ---- per-SST storage ratio (TDefragScanner CRTP hooks) ----
        void OnAddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart* outbound, const TKeyLogoBlob& key,
                const TLevelSegment* sst) {
            if (!ProduceRatios) {
                return;
            }
            RatioKey = key;
            WholeMerger.AddFromSegment(memRec, outbound, key, 0, sst);

            TPerSstCur* cur = nullptr;
            for (TPerSstCur& c : CurSsts) {
                if (c.Sst == sst) {
                    cur = &c;
                    break;
                }
            }
            if (!cur) {
                cur = &CurSsts.emplace_back();
                cur->Sst = sst;
            }
            // subset keep-flag counts, mirroring TRecordMergerBase::AddBasic()
            const int mode = memRec.GetIngress().GetCollectMode(TIngress::IngressMode(GType));
            cur->NumKeepFlags += mode & CollectModeKeep;
            cur->NumDoNotKeepFlags += mode & CollectModeDoNotKeep;

            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);
            cur->InplacedDataSize += extr.GetInplacedDataSize();
            cur->HugeDataSize += extr.GetHugeDataSize();
        }

        void OnAddFromFresh(const TMemRecLogoBlob& memRec, const TKeyLogoBlob& key, ui64 lsn) {
            if (!ProduceRatios) {
                return;
            }
            RatioKey = key;
            WholeMerger.AddFromFresh(memRec, nullptr, key, lsn); // whole-DB counts only; fresh is not an SST
        }

        void OnFinish(const TKeyLogoBlob& /*key*/) {
            if (!ProduceRatios) {
                return;
            }
            WholeMerger.Finish();
            const ui32 wholeKeep = WholeMerger.GetNumKeepFlags();
            const ui32 wholeDoNotKeep = WholeMerger.GetNumDoNotKeepFlags();
            const TMemRecLogoBlob& wholeMemRec = WholeMerger.GetMemRec();
            for (const TPerSstCur& cur : CurSsts) {
                NGc::TKeepStatus keep = Barriers->Keep(RatioKey, wholeMemRec,
                    {cur.NumKeepFlags, cur.NumDoNotKeepFlags >> 1, wholeKeep, wholeDoNotKeep},
                    AllowKeepFlags, true /*allowGarbageCollection*/);
                NHullComp::TSstRatioPtr& ratio = Ratios[cur.Sst];
                if (!ratio) {
                    ratio = MakeIntrusive<NHullComp::TSstRatio>();
                }
                const ui64 indexItemByteSize = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
                ratio->IndexItemsTotal++;
                ratio->IndexBytesTotal += indexItemByteSize;
                ratio->InplacedDataTotal += cur.InplacedDataSize;
                ratio->HugeDataTotal += cur.HugeDataSize;
                if (keep.KeepIndex) {
                    ratio->IndexItemsKeep++;
                    ratio->IndexBytesKeep += indexItemByteSize;
                }
                if (keep.KeepData) {
                    ratio->InplacedDataKeep += cur.InplacedDataSize;
                    ratio->HugeDataKeep += cur.HugeDataSize;
                }
            }
        }

        void OnClear() {
            if (!ProduceRatios) {
                return;
            }
            WholeMerger.Clear();
            CurSsts.clear();
        }

        const THashMap<const TLevelSegment*, NHullComp::TSstRatioPtr>& GetRatios() const {
            return Ratios;
        }

        // Publish the computed per-SST ratios onto the live segments so the compaction selector finds them
        // fresh and skips its own walk. Only call after a COMPLETED scan. StorageRatio is a concurrent-safe
        // in-place cache (spinlock-guarded), so mutating it off the snapshot is safe (same as the selector).
        void ApplyStorageRatios(TInstant now) {
            for (const auto& [sst, ratio] : Ratios) {
                ratio->Time = now;
                // Two-arg Set (NBYDB-1732): also stamps the SST's CalculationTime so the selector sees the
                // ratio as fresh and skips its own recompute. The per-SST jitter that spreads recomputes after
                // a restart lives only in the selector; when this shared pass is on it does the recompute in
                // one walk instead, so the selector stays idle and no jitter is needed here.
                const_cast<TLevelSegment*>(sst)->StorageRatio.Set(ratio, now);
            }
        }
    };

} // NKikimr
