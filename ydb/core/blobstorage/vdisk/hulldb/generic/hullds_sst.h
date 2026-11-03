#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullstorageratio.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <util/generic/set.h>

namespace NKikimr {

    template <class TKey, class TMemRec>
    struct TRecIndex : public TThrRefBase {
        typedef TIndexRecord<TKey, TMemRec> TRec;

        TTrackableVector<TRec> LoadedIndex;

        TRecIndex(TVDiskContextPtr vctx)
            : LoadedIndex(TMemoryConsumer(vctx->SstIndex))
        {}

        bool IsLoaded() const {
            return !LoadedIndex.empty();
        }

        ui64 Elements() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return LoadedIndex.size();
        }
    };

    template <>
    struct TRecIndex<TKeyLogoBlob, TMemRecLogoBlob> : public TThrRefBase {
        typedef TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob> TRec;

        struct TRecHigh {
            ui64 TabletId;
            ui64 ChannelGeneration; // Channel << 32 | Generation, i.e. TLogoBlobID::GetRaw()[1] >> 24
            ui32 LowRangeEndIndex = 0;

            explicit TRecHigh(const TLogoBlobID& id)
                : TabletId(id.GetRaw()[0])
                , ChannelGeneration(id.GetRaw()[1] >> 24)
            {}

            bool SameKey(const TRecHigh& r) const {
                return TabletId == r.TabletId && ChannelGeneration == r.ChannelGeneration;
            }

            bool operator < (const TRecHigh& r) const {
                return TabletId != r.TabletId ? TabletId < r.TabletId : ChannelGeneration < r.ChannelGeneration;
            }
        };

        static_assert(sizeof(TRecHigh) == 24, "expect sizeof(TRecHigh) == 24");
        static_assert(alignof(TRecHigh) == 8, "expect alignof(TRecHigh) == 8");

        // Field order keeps every field naturally aligned;
        // 32-byte alignment keeps every record within a cache line.
        struct alignas(32) TRecLow {
            ui64 Raw2; // TLogoBlobID::GetRaw()[2]: Step & 0xFF | Cookie | CrcMode | BlobSize | PartId
            TMemRecLogoBlob MemRec;
            ui32 Step;

            explicit TRecLow(const TLogoBlobID& id, const TMemRecLogoBlob& memRec = {})
                : Raw2(id.GetRaw()[2])
                , MemRec(memRec)
                , Step(id.Step())
            {}

            const TMemRecLogoBlob& GetMemRec() const {
                return MemRec;
            }

            bool operator < (const TRecLow& r) const {
                return Step != r.Step ? Step < r.Step : Raw2 < r.Raw2;
            }
        };

        static_assert(sizeof(TRecLow) == 32, "expect sizeof(TRecLow) == 32");
        static_assert(alignof(TRecLow) == 32, "expect alignof(TRecLow) == 32");
        static_assert(offsetof(TRecLow, MemRec) == 8, "expect offsetof(TRecLow, MemRec) == 8");

        static TLogoBlobID MakeLogoBlobId(const TRecHigh& high, const TRecLow& low) {
            return TLogoBlobID(high.TabletId, high.ChannelGeneration << 24 | low.Step >> 8, low.Raw2);
        }

        TTrackableVector<TRecHigh> IndexHigh;
        TTrackableVector<TRecLow> IndexLow;

        TRecIndex(TVDiskContextPtr vctx)
            : IndexHigh(TMemoryConsumer(vctx->SstIndex))
            , IndexLow(TMemoryConsumer(vctx->SstIndex))
        {}

        bool IsLoaded() const {
            return !IndexLow.empty();
        }

        ui64 Elements() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return IndexLow.size();
        }

        void LoadLinearIndex(const TTrackableVector<TRec>& linearIndex) {
            if (linearIndex.empty()) {
                return;
            }

            // count high records first to allocate IndexHigh exactly
            size_t highCount = 1;
            TRecHigh highPrev(linearIndex.begin()->GetKey().LogoBlobID());
            for (const TRec* rec = linearIndex.begin() + 1; rec != linearIndex.end(); ++rec) {
                TRecHigh high(rec->GetKey().LogoBlobID());
                if (!high.SameKey(highPrev)) {
                    ++highCount;
                    highPrev = high;
                }
            }

            IndexHigh.clear();
            IndexHigh.reserve(highCount);
            IndexLow.clear();
            IndexLow.reserve(linearIndex.size());

            for (const TRec& rec : linearIndex) {
                const TLogoBlobID blobId = rec.GetKey().LogoBlobID();
                TRecHigh high(blobId);
                if (IndexHigh.empty() || !high.SameKey(IndexHigh.back())) {
                    if (!IndexHigh.empty()) {
                        IndexHigh.back().LowRangeEndIndex = IndexLow.size();
                    }
                    IndexHigh.push_back(high);
                }
                IndexLow.emplace_back(blobId, rec.GetMemRec());
            }

            IndexHigh.back().LowRangeEndIndex = IndexLow.size();
            Y_DEBUG_ABORT_UNLESS(IndexHigh.size() == highCount);
        }

        void SaveLinearIndex(TTrackableVector<TRec>* linearIndex) const {
            if (IndexLow.empty()) {
                return;
            }

            linearIndex->clear();
            linearIndex->reserve(IndexLow.size());

            const TRecHigh* high = IndexHigh.begin();
            const TRecLow* low = IndexLow.begin();
            const TRecLow* lowRangeEnd = low + high->LowRangeEndIndex;

            while (low != IndexLow.end()) {
                linearIndex->emplace_back(TKeyLogoBlob(MakeLogoBlobId(*high, *low)), low->GetMemRec());

                ++low;
                if (Y_UNLIKELY(low == lowRangeEnd)) {
                    ++high;
                    if (high != IndexHigh.end()) {
                        lowRangeEnd = IndexLow.begin() + high->LowRangeEndIndex;
                    }
                }
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelSegment : public TRecIndex<TKey, TMemRec> {
        typedef TLevelSegment<TKey, TMemRec> TThis;
        using TKeyType = TKey;
        using TMemRecType = TMemRec;
        struct TLevelSstPtr {
            ui32 Level = 0;
            TIntrusivePtr<TThis> SstPtr;

            TLevelSstPtr() = default;
            TLevelSstPtr(ui32 level, const TIntrusivePtr<TThis> &sstPtr)
                : Level(level)
                , SstPtr(sstPtr)
            {}

            bool operator < (const TLevelSstPtr &x) const {
                return (Level < x.Level) || (Level == x.Level && LessAtSameLevel(x));
            }

            bool IsSameSst(const TLevelSstPtr &x) const {
                bool equal = SstPtr.Get() == x.SstPtr.Get();
                Y_ABORT_UNLESS(!equal || (equal && Level == x.Level));
                return equal;
            }

            TString ToString() const {
                return Sprintf("[%u %u (%s-%s)]", Level, SstPtr->GetFirstLsn(), SstPtr->FirstKey().ToString().data(), SstPtr->LastKey().ToString().data());
            }

        private:
            bool LessAtSameLevel(const TLevelSstPtr &x) const {
                if (Level == 0) {
                    Y_ABORT_UNLESS(SstPtr->VolatileOrderId != 0 && x.SstPtr->VolatileOrderId != 0);
                    // unordered level, compare by VolatileOrderId that grows sequentially
                    return SstPtr->VolatileOrderId < x.SstPtr->VolatileOrderId;
                } else {
                    // sorted level, compare by key
                    return SstPtr->FirstKey() < x.SstPtr->FirstKey();
                }
            }
        };

        TDiskPart LastPartAddr; // tail of reverted list of parts (on disk)
        TTrackableVector<TDiskPart> LoadedOutbound;
        TIdxDiskPlaceHolder::TInfo Info;
        TVector<ui32> AllChunks;    // all chunk ids that store index and data for this segment
        TDiskPart HeapStripe; // non-empty if this SST lives in the stripe heap
        std::vector<TDiskPart> IndexParts; // index part locations; the first one is 'placeholder', stored as LastPartAddr
        NHullComp::TSstRatioThreadSafeHolder StorageRatio;
        // Every Sst has unique id
        ui64 AssignedSstId = 0;
        // Every Sst at Level 0 has volatile growing id
        ui64 VolatileOrderId = 0;

        TLevelSegment(TVDiskContextPtr vctx)
            : TRecIndex<TKey, TMemRec>(vctx)
            , LastPartAddr()
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {}

        TLevelSegment(TVDiskContextPtr vctx, const TDiskPart &addr)
            : TRecIndex<TKey, TMemRec>(vctx)
            , LastPartAddr(addr)
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {
            Y_DEBUG_ABORT_UNLESS(!addr.Empty());
        }

        TLevelSegment(TVDiskContextPtr vctx, const NKikimrVDiskData::TDiskPart &pb)
            : TRecIndex<TKey, TMemRec>(vctx)
            , LastPartAddr(pb)
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {
            // HeapStripe is not stored: it is derived from chunk ownership by ResolveHeapStripe() once the huge
            // keeper has been recovered
        }

        // A stripe-backed SST is written as a single index part filling its whole stripe, so the stripe extent is
        // just the SST address. Ownership of the chunk is what tells the two kinds of SST apart.
        void ResolveHeapStripe(const THashSet<TChunkIdx>& stripeChunks) {
            HeapStripe = stripeChunks.contains(LastPartAddr.ChunkIdx) ? LastPartAddr : TDiskPart();
        }

        const TDiskPart &GetEntryPoint() const {
            return LastPartAddr;
        }

        void SetAddr(const TDiskPart &addr) {
            LastPartAddr = addr;
        }

        const TDiskPart *GetOutbound() const {
            return LoadedOutbound.data();
        }

        TString ChunksToString() const {
            TStringStream str;
            for (auto x : AllChunks) {
                str << x << " ";
            }
            return str.Str();
        }

        void SerializeToProto(NKikimrVDiskData::TDiskPart &pb) const {
            LastPartAddr.SerializeToProto(pb);
        }

        // Every huge blob this SST references. Chunk ownership and stripe extents are both derived from this one
        // traversal, so the set of chunks the hull claims can never disagree with the extents it keeps alive.
        template<typename TCallback>
        void ForEachHugeBlob(TCallback&& callback) const {
            TDiskDataExtractor extr;
            TMemIterator it(this);
            it.SeekToFirst();
            while (it.Valid()) {
                const TMemRec& memRec = it.GetMemRec();
                switch (memRec.GetType()) {
                    case TBlobType::HugeBlob:
                    case TBlobType::ManyHugeBlobs:
                        it.GetDiskData(&extr);
                        for (const TDiskPart *part = extr.Begin; part != extr.End; ++part) {
                            if (part->Size) {
                                Y_ABORT_UNLESS(part->ChunkIdx);
                                callback(*part);
                            }
                        }
                        extr.Clear();
                        break;

                    case TBlobType::MemBlob:
                    case TBlobType::DiskBlob:
                        break;
                }
                it.Next();
            }
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            // here we handle SST itself (index part) + any referenced data chunks
            for (TChunkIdx chunkIdx : AllChunks) {
                const bool inserted = chunks.insert(chunkIdx).second;
                // Heap-backed SSTs share the chunk with the stripe heap.
                Y_ABORT_UNLESS(inserted || !HeapStripe.Empty());
            }

            ForEachHugeBlob([&chunks](const TDiskPart& part) { chunks.insert(part.ChunkIdx); });
        }

        // Every extent of this SST that lives in the stripe heap: the huge blobs it points at, plus the stripe the
        // SST itself occupies. Must run after ResolveHeapStripe, which is what fills HeapStripe in.
        template<typename TCallback>
        void ForEachStripeExtent(const THashSet<TChunkIdx>& stripeChunks, TCallback&& callback) const {
            if (!HeapStripe.Empty()) {
                callback(HeapStripe);
            }
            ForEachHugeBlob([&](const TDiskPart& part) {
                if (stripeChunks.contains(part.ChunkIdx)) {
                    callback(part);
                }
            });
        }

        class TMemIterator;

        ui64 GetFirstLsn() const { return Info.FirstLsn; }
        ui64 GetLastLsn() const { return Info.LastLsn; }
        TKey FirstKey() const;
        TKey LastKey() const;

        // append cur seg chunk ids (index and data) to the vector
        void FillInChunkIds(TVector<ui32> &vec) const {
            if (!HeapStripe.Empty()) {
                return;
            }
            // copy chunks ids
            for (auto idx : AllChunks)
                vec.push_back(idx);
        }
        void OutputHtml(ui32 &index, ui32 level, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const;

        void OutputProto(ui32 level, google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const;
        // dump all accessible data
        void DumpAll(IOutputStream &str) const {
            str << "=== SST ===\n";
            // We can add dump of SST here to be more verbose
        }
        void Output(IOutputStream &str) const;

        class TBaseWriter;
        class TDataWriter;
        class TIndexBuilder;
        class TWriter;
    };

    extern template struct TLevelSegment<TKeyBarrier, TMemRecBarrier>;
    extern template struct TLevelSegment<TKeyBlock, TMemRecBlock>;

} // NKikimr
