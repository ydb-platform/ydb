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

    template <class TKey, class TMemRec>
    struct TRecIndexBase : public TThrRefBase {
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
                bool operator ()(const TRec &x, const TKey &key) const {
                    return x.Key < key;
                }
            };
        };
#pragma pack(pop)
    };

    template <class TKey, class TMemRec>
    struct TRecIndex
        : public TRecIndexBase<TKey, TMemRec>
    {
        typedef TRecIndexBase<TKey, TMemRec>::TRec TRec;

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
    struct TRecIndex<TKeyLogoBlob, TMemRecLogoBlob>
        : public TRecIndexBase<TKeyLogoBlob, TMemRecLogoBlob>
    {
#pragma pack(push, 4)
        struct TLogoBlobIdHigh {
            union {
                struct {
                    ui64 TabletId; // 8 bytes
                    ui64 StepR1 : 24; // 8 bytes
                    ui64 Generation : 32;
                    ui64 Channel : 8;
                } N;

                ui64 X[2];
            } Raw;

            TLogoBlobIdHigh() {
                Raw.X[0] = Raw.X[1] = 0;
            }

            explicit TLogoBlobIdHigh(const TLogoBlobID& id) {
                Raw.X[0] = id.GetRaw()[0];
                Raw.X[1] = id.GetRaw()[1];
            }

            TLogoBlobIdHigh(ui64 tabletId, ui32 generation, ui32 step, ui8 channel) {
                Raw.N.TabletId = tabletId;
                Raw.N.Channel = channel;
                Raw.N.Generation = generation;
                Raw.N.StepR1 = (step & 0xFFFFFF00ull) >> 8;
            }

            bool operator == (const TLogoBlobIdHigh& r) const {
                return Raw.X[0] == r.Raw.X[0] && Raw.X[1] == r.Raw.X[1];
            }

            bool operator != (const TLogoBlobIdHigh& r) const {
                return !(operator == (r));
            }

            bool operator < (const TLogoBlobIdHigh& r) const {
                return Raw.X[0] != r.Raw.X[0] ? Raw.X[0] < r.Raw.X[0] : Raw.X[1] < r.Raw.X[1];
            }
        };

        static_assert(sizeof(TLogoBlobIdHigh) == 16, "expect sizeof(TLogoBlobIdHigh) == 16");

        struct TLogoBlobIdLow {
            union {
                struct {
                    ui64 PartId : 4; // 8 bytes
                    ui64 BlobSize : 26;
                    ui64 CrcMode : 2;
                    ui64 Cookie : 24;
                    ui64 StepR2 : 8;
                } N;

                ui64 X;
            } Raw;

            explicit TLogoBlobIdLow(const TLogoBlobID& id) {
                Raw.X = id.GetRaw()[2];
            }

            TLogoBlobIdLow(ui32 step, ui32 cookie, ui32 crcMode, ui32 blobSize, ui32 partId) {
                Raw.N.StepR2 = step & 0x000000FFull;
                Raw.N.Cookie = cookie;
                Raw.N.CrcMode = crcMode;
                Raw.N.BlobSize = blobSize;
                Raw.N.PartId = partId;
            }

            bool operator == (const TLogoBlobIdLow& r) const {
                return Raw.X == r.Raw.X;
            }

            bool operator != (const TLogoBlobIdLow& r) const {
                return !(operator == (r));
            }

            bool operator < (const TLogoBlobIdLow& r) const {
                return Raw.X < r.Raw.X;
            }
        };

        static_assert(sizeof(TLogoBlobIdLow) == 8, "expect sizeof(TLogoBlobIdLow) == 8");

        struct TRecHigh {
            TLogoBlobIdHigh Key;
            ui32 LowRangeEndIndex;

            TRecHigh(TLogoBlobIdHigh key)
                : Key(key)
            {}

            struct TLess {
                bool operator ()(const TRecHigh& l, const TLogoBlobIdHigh& r) const {
                    return l.Key < r;
                }
            };
        };

        static_assert(sizeof(TRecHigh) == 20, "expect sizeof(TRecHigh) == 20");

        struct TRecLow {
            TLogoBlobIdLow Key;
            TMemRecLogoBlob MemRec;

            TRecLow(TLogoBlobIdLow key, TMemRecLogoBlob memRec)
                : Key(key)
                , MemRec(memRec)
            {}

            struct TLess {
                bool operator ()(const TRecLow& l, const TLogoBlobIdLow& r) const {
                    return l.Key < r;
                }
            };
        };

        static_assert(sizeof(TRecLow) == 28, "expect sizeof(TRecLow) == 28");
#pragma pack(pop)

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

            IndexHigh.clear();
            IndexHigh.reserve(linearIndex.size());
            IndexLow.clear();
            IndexLow.reserve(linearIndex.size());

            const TRec* rec = linearIndex.begin();

            auto blobId = rec->Key.LogoBlobID();
            TLogoBlobIdHigh high(blobId);
            TLogoBlobIdLow low(blobId);
            TLogoBlobIdHigh highPrev = high;

            IndexHigh.emplace_back(high);
            IndexLow.emplace_back(low, rec->MemRec);
            ++rec;

            for (; rec != linearIndex.end(); ++rec) {
                auto blobId = rec->Key.LogoBlobID();
                TLogoBlobIdHigh high(blobId);
                TLogoBlobIdLow low(blobId);

                if (Y_UNLIKELY(high != highPrev)) {
                    IndexHigh.back().LowRangeEndIndex = IndexLow.size();
                    IndexHigh.emplace_back(high);
                    highPrev = high;
                }

                IndexLow.emplace_back(low, rec->MemRec);
            }

            IndexHigh.back().LowRangeEndIndex = IndexLow.size();
            IndexHigh.shrink_to_fit();
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
                auto& highKey = high->Key;
                TLogoBlobID blobId(highKey.Raw.X[0], highKey.Raw.X[1], low->Key.Raw.X);
                linearIndex->emplace_back(TKeyLogoBlob(blobId), low->MemRec);

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
        {}

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
                const TMemRec& memRec = it.GetMemRec();
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
        TKey FirstKey() const;
        TKey LastKey() const;

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

    extern template struct TLevelSegment<TKeyBarrier, TMemRecBarrier>;
    extern template struct TLevelSegment<TKeyBlock, TMemRecBlock>;

} // NKikimr
