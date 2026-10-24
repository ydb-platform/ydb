#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullstorageratio.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include "hullds_sst_twolevel.h"
#include "hullds_sst_packed.h"
#include <util/generic/set.h>
#include <variant>

namespace NKikimr {

    template <class TKey, class TMemRec>
    struct TFlatSstIndex {
        using TRec = TIndexRecord<TKey, TMemRec>;

        TTrackableVector<TRec> Recs;

        TFlatSstIndex(TVDiskContextPtr vctx)
            : Recs(TMemoryConsumer(vctx->SstIndex))
        {}

        bool IsLoaded() const {
            return !Recs.empty();
        }

        ui64 Elements() const {
            return Recs.size();
        }

        TKey FirstKey() const {
            return Recs.front().GetKey();
        }

        TKey LastKey() const {
            return Recs.back().GetKey();
        }

        void Load(TTrackableVector<TRec> linearIndex) {
            Recs = std::move(linearIndex);
            Recs.shrink_to_fit();
        }

        void Save(TTrackableVector<TRec>* linearIndex) const {
            linearIndex->assign(Recs.begin(), Recs.end());
        }

        class TIterator;
    };

    // The LogoBlob index in the form VCtx->SstIndexFormPolicy selected when the SST was created.
    class TLogoBlobSstIndex {
    public:
        using TRec = TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>;
        using TForms = std::variant<const TTwoLevelLogoBlobSstIndex, const TPackedLogoBlobSstIndex>;

        class TIterator;

        TLogoBlobSstIndex(TVDiskContextPtr vctx)
            : Consumer(vctx->SstIndex)
            , Policy(vctx->SstIndexFormPolicy)
            , Form(std::in_place_type<const TTwoLevelLogoBlobSstIndex>, Consumer, TTrackableVector<TRec>(TMemoryConsumer(Consumer)))
        {}

        bool IsLoaded() const {
            return Elements() != 0;
        }

        ui64 Elements() const {
            return std::visit([](const auto& form) { return form.Elements(); }, Form);
        }

        TKeyLogoBlob FirstKey() const {
            return std::visit([](const auto& form) { return form.FirstKey(); }, Form);
        }

        TKeyLogoBlob LastKey() const {
            return std::visit([](const auto& form) { return form.LastKey(); }, Form);
        }

        // Called once, before any iterator is created:
        // TIterator binds to the form, and a later Load would destroy that form under it.
        void Load(TTrackableVector<TRec> linearIndex) {
            Y_ABORT_UNLESS(!IsLoaded(), "SST index is loaded twice");

            // The packed form cannot be empty, so an empty index is two-level whatever the policy; 
            // SST is not expected to have no records anyway.
            if (Policy == ESstIndexFormPolicy::Packed && !linearIndex.empty()) {
                Form.emplace<const TPackedLogoBlobSstIndex>(Consumer, linearIndex);
            } else {
                Form.emplace<const TTwoLevelLogoBlobSstIndex>(Consumer, linearIndex);
            }
        }

        void Save(TTrackableVector<TRec>* linearIndex) const {
            std::visit([&](const auto& form) { form.Save(linearIndex); }, Form);
        }

    private:
        const TMemoryConsumer Consumer;
        const ESstIndexFormPolicy Policy;
        TForms Form;
    };

    // Which index an SST of the given key type keeps in memory: the flat array for everything but LogoBlobs.
    template <class TKey, class TMemRec>
    struct TSstIndexTraits {
        using TIndex = TFlatSstIndex<TKey, TMemRec>;
    };

    template <>
    struct TSstIndexTraits<TKeyLogoBlob, TMemRecLogoBlob> {
        using TIndex = TLogoBlobSstIndex;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelSegment : public TThrRefBase {
        typedef TLevelSegment<TKey, TMemRec> TThis;
        using TKeyType = TKey;
        using TMemRecType = TMemRec;
        using TRec = TIndexRecord<TKey, TMemRec>;
        using TIndex = typename TSstIndexTraits<TKey, TMemRec>::TIndex;
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

        TIndex Index; // in-memory index
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
            : Index(vctx)
            , LastPartAddr()
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {}

        TLevelSegment(TVDiskContextPtr vctx, const TDiskPart &addr)
            : Index(vctx)
            , LastPartAddr(addr)
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {
            Y_DEBUG_ABORT_UNLESS(!addr.Empty());
        }

        TLevelSegment(TVDiskContextPtr vctx, const NKikimrVDiskData::TDiskPart &pb)
            : Index(vctx)
            , LastPartAddr(pb)
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {
            // HeapStripe is not stored: it is derived from chunk ownership by ResolveHeapStripe() once the huge
            // keeper has been recovered
        }

        bool IsLoaded() const {
            return Index.IsLoaded();
        }

        ui64 Elements() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return Index.Elements();
        }

        void LoadLinearIndex(TTrackableVector<TRec> linearIndex) {
            Index.Load(std::move(linearIndex));
        }

        void SaveLinearIndex(TTrackableVector<TRec>* linearIndex) const {
            Index.Save(linearIndex);
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
        TKey FirstKey() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return Index.FirstKey();
        }

        TKey LastKey() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return Index.LastKey();
        }

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
