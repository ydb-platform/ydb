#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_defrag.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
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
        THashSet<TChunkIdx> ChunksToShred;

        static TChunksToDefrag Shred(const auto& chunks) {
            TChunksToDefrag res;
            res.ChunksToShred = {chunks.begin(), chunks.end()};
            return res;
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

        bool IsShred() const {
            return !ChunksToShred.empty();
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
        THullDsSnap FullSnap;
        const TBlobStorageGroupType GType;
        const TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Barriers;
        const bool AllowKeepFlags;
        TLogoBlobsSnapshot::TForwardIterator Iter;

        TDataMerger Merger;
        TKeyLogoBlob Key;
        TMemRecLogoBlob MemRec;

        std::optional<THashSet<ui32>> Filter;

    public:
        TDefragScanner(THullDsSnap&& fullSnap, std::optional<THashSet<TChunkIdx>>&& filter)
            : FullSnap(std::move(fullSnap))
            , GType(FullSnap.HullCtx->VCtx->Top->GType)
            , Barriers(FullSnap.BarriersSnap.CreateEssence(FullSnap.HullCtx))
            , AllowKeepFlags(FullSnap.HullCtx->AllowKeepFlags)
            , Iter(FullSnap.HullCtx, &FullSnap.LogoBlobsSnap)
            , Merger(GType, false /* addHeader doesn't really matter here */)
            , Filter(std::move(filter))
        {
            Iter.SeekToFirst();
        }

        bool Scan(TDuration maxTime) {
            ui64 endTime = GetCycleCountFast() + DurationToCycles(maxTime);
            ui32 count = 0;
            for (; Iter.Valid(); Iter.Next()) {
                if (++count % 1024 == 0 && GetCycleCountFast() >= endTime) {
                    break;
                }
                Start(Iter.GetCurKey());
                Iter.PutToMerger(this);
                Finish();
            }
            return Iter.Valid();
        }

        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* /*data*/, const TKeyLogoBlob& key, ui64 lsn) {
            Update(memRec, nullptr, lsn);
            MemRec.Merge(memRec, key, false, GType);
        }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key, ui64 circaLsn,
                const void* /*sst*/) {
            Update(memRec, outbound, circaLsn);
            MemRec.Merge(memRec, key, false, GType);
        }

        static constexpr bool HaveToMergeData() { return false; }

    private:
        void Start(const TKeyLogoBlob& key) {
            Merger.Clear();
            Key = key;
            MemRec = {};
        }

        void Finish() {
            Merger.Finish(true, Key.LogoBlobID());
            if (!Merger.Empty()) {
                NGc::TKeepStatus status = Barriers->Keep(Key, MemRec, {}, AllowKeepFlags, true /*allowGarbageCollection*/);
                for (const TDiskPart& part : Merger.GetSavedHugeBlobs()) {
                    if (!part.Empty() && (!Filter || Filter->contains(part.ChunkIdx))) {
                        static_cast<TDerived&>(*this).Add(part, Key.LogoBlobID(), status.KeepData);
                    }
                }
                for (const TDiskPart& part : Merger.GetDeletedHugeBlobs()) {
                    Y_ABORT_UNLESS(!part.Empty());
                    if (!Filter || Filter->contains(part.ChunkIdx)) {
                        static_cast<TDerived&>(*this).Add(part, Key.LogoBlobID(), false);
                    }
                }
            }
            Merger.Clear();
        }

        void Update(const TMemRecLogoBlob &memRec, const TDiskPart *outbound, ui64 lsn) {
            if (memRec.GetType() == TBlobType::HugeBlob || memRec.GetType() == TBlobType::ManyHugeBlobs) {
                TDiskDataExtractor extr;
                memRec.GetDiskData(&extr, outbound);
                Merger.AddHugeBlob(extr.Begin, extr.End, memRec.GetIngress().LocalParts(GType), lsn);
            }
        }
    };

    class TDefragQuantumChunkFinder {
    private:
        // Info gathered per chunk
        struct TChunkInfo {
            ui32 UsefulSlots = 0;
            const ui32 SlotSize;
            const ui32 NumberOfSlotsInChunk;

            TChunkInfo(ui32 slotSize, ui32 numberOfSlotsInChunk)
                : SlotSize(slotSize)
                , NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}

            TString ToString() const {
                TStringStream str;
                str << "UsefulSlots# " << UsefulSlots << "/" << NumberOfSlotsInChunk;
                return str.Str();
            }
        };

        // Aggregated info gathered per slotSize
        struct TAggrSlotInfo {
            ui64 UsefulSlots = 0;
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
                    aggr.UsefulSlots += chunk.UsefulSlots;
                    ++aggr.UsedChunks;
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
                    auto it = aggrSlots.find(chunk.SlotSize);
                    Y_ABORT_UNLESS(it != aggrSlots.end());
                    auto& a = it->second;

                    // if we can put all current used slots into UsedChunks - 1, then defragment this chunk
                    if (a.NumberOfSlotsInChunk * (a.UsedChunks - 1) >= a.UsefulSlots) {
                        --a.UsedChunks;
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

        void Add(TDiskPart part, const TLogoBlobID& id, bool useful) {
            ChunksMap.Add(part, id, useful);
        }
    };

    class TDefragQuantumFindChunks
        : public TDefragQuantumChunkFinder
        , public TDefragScanner<TDefragQuantumFindChunks>
    {
    public:
        TDefragQuantumFindChunks(THullDsSnap&& snap, const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx,
                std::optional<THashSet<TChunkIdx>>&& chunksToShred)
            : TDefragQuantumChunkFinder(hugeBlobCtx)
            , TDefragScanner(std::move(snap), std::move(chunksToShred))
        {}

        using TDefragQuantumChunkFinder::Add;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TDefragQuantumFindRecords {
        using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;

        THashSet<ui32> Chunks; // chunks to defrag (i.e. move all data from these chunks)
        std::vector<TDefragRecord> RecsToRewrite;
        std::optional<TLogoBlobID> NextId;
        const TBlobStorageGroupType GType;
        TDataMerger DataMerger;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Barriers;
        TLogoBlobID FullId;
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const ui32 MinHugeBlobInBytes;
        bool AllowKeepFlags;
        THashSet<ui64> TablesToCompact;

    public:
        TDefragQuantumFindRecords(TChunksToDefrag&& chunksToDefrag, TBlobStorageGroupType gtype, bool addHeader,
                std::shared_ptr<THugeBlobCtx> hugeBlobCtx, ui32 minHugeBlobInBytes)
            : GType(gtype)
            , DataMerger(gtype, addHeader)
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , MinHugeBlobInBytes(minHugeBlobInBytes)
        {
            if (chunksToDefrag.IsShred()) {
                Chunks = std::move(chunksToDefrag.ChunksToShred);
            } else {
                for (const auto& chunk : chunksToDefrag.Chunks) {
                    Chunks.insert(chunk.ChunkId);
                }
                Y_ABORT_UNLESS(Chunks.size() == chunksToDefrag.Chunks.size()); // ensure there are no duplicate numbers
            }
        }

        bool Scan(TDuration quota, THullDsSnap fullSnap) {
            // create barrier essence to filter out unneeded blobs
            Barriers = fullSnap.BarriersSnap.CreateEssence(fullSnap.HullCtx);
            AllowKeepFlags = fullSnap.HullCtx->AllowKeepFlags;
            // create iterator and set it up to point to next blob of interest
            TLogoBlobsSnapshot::TForwardIterator iter(fullSnap.HullCtx, &fullSnap.LogoBlobsSnap);
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt(&iter);
            // calculate timestamp to finish scanning
            const ui64 endTime = GetCycleCountFast() + DurationToCycles(quota);
            ui32 count = 0;
            auto callback = [&](TKeyLogoBlob /*key*/, auto* /*merger*/) -> bool {
                return (++count % 1024 != 0 || GetCycleCountFast() < endTime);
            };
            if (NextId) {
                heapIt.Seek(*NextId);
            } else {
                heapIt.SeekToFirst();
            }
            heapIt.Walk(std::nullopt, this, callback);
            if (heapIt.Valid()) {
                NextId.emplace(heapIt.GetCurKey().LogoBlobID());
            }
            return heapIt.Valid();
        }

        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* /*data*/, const TKeyLogoBlob& key, ui64 lsn) {
            Update(key, memRec, nullptr, lsn, nullptr);
        }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key, ui64 circaLsn,
                const TLevelSegment *sst) {
            Update(key, memRec, outbound, circaLsn, sst);
        }

        void Finish() {
            DataMerger.Finish(HugeBlobCtx->IsHugeBlob(GType, FullId, MinHugeBlobInBytes), FullId);

            const auto& savedHugeBlobs = DataMerger.GetSavedHugeBlobs();
            size_t index = 0;
            for (ui8 partIdx : DataMerger.GetParts()) {
                Y_DEBUG_ABORT_UNLESS(index != savedHugeBlobs.size());
                if (const TDiskPart& part = savedHugeBlobs[index++]; part.ChunkIdx && Chunks.contains(part.ChunkIdx)) {
                    Y_DEBUG_ABORT_UNLESS(FullId);
                    RecsToRewrite.emplace_back(TLogoBlobID(FullId, partIdx + 1), part);
                }
            }
            Y_DEBUG_ABORT_UNLESS(index == savedHugeBlobs.size());
        }

        void Clear() {
            DataMerger.Clear();
            FullId = {};
        }

        static constexpr bool HaveToMergeData() { return false; }
        std::vector<TDefragRecord> GetRecordsToRewrite() { return std::move(RecsToRewrite); }
        THashSet<ui64>& GetTablesToCompact() { return TablesToCompact; }

    private:
        void Update(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec, const TDiskPart *outbound, ui64 lsn,
                const TLevelSegment *sst) {
            FullId = key.LogoBlobID();

            if (memRec.GetType() != TBlobType::HugeBlob && memRec.GetType() != TBlobType::ManyHugeBlobs) {
                return;
            }

            // remember all the SSTables containing references to chunks being defragmented/shredded
            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);
            if (sst) {
                for (const TDiskPart *p = extr.Begin; p != extr.End; ++p) {
                    if (p->ChunkIdx && Chunks.contains(p->ChunkIdx)) {
                        TablesToCompact.insert(sst->AssignedSstId);
                    }
                }
            }

            if (const auto& keep = Barriers->Keep(key, memRec, {}, AllowKeepFlags, true); !keep.KeepData) {
                return;
            }

            DataMerger.AddHugeBlob(extr.Begin, extr.End, memRec.GetLocalParts(GType), lsn);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TDefragCalcStat
        : public TDefragScanner<TDefragCalcStat>
        , public TDefragQuantumChunkFinder
    {
        struct TPerSlotSizeInfo {
            ui32 UsefulSlots = 0;
            const ui32 NumberOfSlotsInChunk;

            TPerSlotSizeInfo(ui32 numberOfSlotsInChunk)
                : NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}
        };

        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        std::unordered_set<ui32> Chunks;
        std::unordered_map<ui32, ui32> Map; // numberOfSlotsInChunk -> usefulSlots

    public:
        TDefragCalcStat(THullDsSnap&& fullSnap, const std::shared_ptr<THugeBlobCtx>& hugeBlobCtx)
            : TDefragScanner(std::move(fullSnap), std::nullopt)
            , TDefragQuantumChunkFinder(hugeBlobCtx)
            , HugeBlobCtx(hugeBlobCtx)
        {}

        void Add(TDiskPart part, const TLogoBlobID& id, bool useful) {
            Chunks.insert(part.ChunkIdx);
            if (useful) {
                const THugeSlotsMap::TSlotInfo *slotInfo = HugeBlobCtx->HugeSlotsMap->GetSlotInfo(part.Size);
                Y_ABORT_UNLESS(slotInfo, "size# %" PRIu32, part.Size);
                ++Map[slotInfo->NumberOfSlotsInChunk];
            }
            TDefragQuantumChunkFinder::Add(part, id, useful);
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
    };

} // NKikimr
