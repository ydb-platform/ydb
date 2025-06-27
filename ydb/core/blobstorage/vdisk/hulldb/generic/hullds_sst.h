#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullstorageratio.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <util/generic/set.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSegment
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelSegment : public TThrRefBase {
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
                return Sprintf("[%u %lu (%s-%s)]", Level, SstPtr->GetFirstLsn(), SstPtr->FirstKey().ToString().data(), SstPtr->LastKey().ToString().data());
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

        // records stored in the index
#pragma pack(push, 4)
        struct TRec {
            TKey Key;
            TMemRec MemRec;

            TRec() = default;

            TRec(const TKey &key)
                : Key(key)
                , MemRec()
            {}

            TRec(const TKey &key, const TMemRec &memRec)
                : Key(key)
                , MemRec(memRec)
            {}

            struct TLess {
                bool operator () (const TRec &x, const TKey &key) const {
                    return x.Key < key;
                }
            };
        };
#pragma pack(pop)

        TDiskPart LastPartAddr; // tail of reverted list of parts (on disk)
        TTrackableVector<TRec> LoadedIndex;    // the whole index loaded into memory
        TTrackableVector<TDiskPart> LoadedOutbound;
        TIdxDiskPlaceHolder::TInfo Info;
        TVector<ui32> AllChunks;    // all chunk ids that store index and data for this segment
        std::vector<TDiskPart> IndexParts; // index part locations; the first one is 'placeholder', stored as LastPartAddr
        NHullComp::TSstRatioThreadSafeHolder StorageRatio;
        // Every Sst has unique id
        ui64 AssignedSstId = 0;
        // Every Sst at Level 0 has volatile growing id
        ui64 VolatileOrderId = 0;

        TLevelSegment(TVDiskContextPtr vctx)
            : LastPartAddr()
            , LoadedIndex(TMemoryConsumer(vctx->SstIndex))
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {}

        TLevelSegment(TVDiskContextPtr vctx, const TDiskPart &addr)
            : LastPartAddr(addr)
            , LoadedIndex(TMemoryConsumer(vctx->SstIndex))
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {
            Y_DEBUG_ABORT_UNLESS(!addr.Empty());
        }

        TLevelSegment(TVDiskContextPtr vctx, const NKikimrVDiskData::TDiskPart &pb)
            : LastPartAddr(pb)
            , LoadedIndex(TMemoryConsumer(vctx->SstIndex))
            , LoadedOutbound(TMemoryConsumer(vctx->SstIndex))
            , Info()
            , AllChunks()
            , StorageRatio()
        {}

        const TDiskPart &GetEntryPoint() const {
            return LastPartAddr;
        }

        bool IsLoaded() const {
            return !LoadedIndex.empty();
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

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            // here we handle SST itself (index part) + any referenced data chunks
            for (TChunkIdx chunkIdx : AllChunks) {
                const bool inserted = chunks.insert(chunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }

            // iterate through referenced huge blobs and add their chunks to map
            TDiskDataExtractor extr;
            TMemIterator it(this);
            it.SeekToFirst();
            while (it.Valid()) {
                const TMemRec& memRec = it->MemRec;
                switch (memRec.GetType()) {
                    case TBlobType::HugeBlob:
                    case TBlobType::ManyHugeBlobs:
                        it.GetDiskData(&extr);
                        for (const TDiskPart *part = extr.Begin; part != extr.End; ++part) {
                            if (part->Size) {
                                Y_ABORT_UNLESS(part->ChunkIdx);
                                chunks.insert(part->ChunkIdx);
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

        class TMemIterator;

        ui64 GetFirstLsn() const { return Info.FirstLsn; }
        ui64 GetLastLsn() const { return Info.LastLsn; }
        const TKey &FirstKey() const;
        const TKey &LastKey() const;
        // number of elements in the sst
        ui64 Elements() const {
            Y_DEBUG_ABORT_UNLESS(IsLoaded());
            return LoadedIndex.size();
        }
        // append cur seg chunk ids (index and data) to the vector
        void FillInChunkIds(TVector<ui32> &vec) const {
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

    extern template struct TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template struct TLevelSegment<TKeyBarrier, TMemRecBarrier>;
    extern template struct TLevelSegment<TKeyBlock, TMemRecBlock>;

} // NKikimr

