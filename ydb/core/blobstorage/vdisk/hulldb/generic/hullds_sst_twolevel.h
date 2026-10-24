#pragma once

#include "defs.h"
#include "hullds_sst_indexhigh.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TTwoLevelLogoBlobSstIndex -- the LogoBlob SST index in two uncoded levels
    //
    // TRecHigh per group of records with the same TabletId, Channel and Generation.
    // TRecLow per record with the rest of the key and the memrec.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TTwoLevelLogoBlobSstIndex {
        using TRec = TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>;

        using TRecHigh = TLogoBlobSstRecHigh;

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

        TTrackableVector<TRecHigh> IndexHigh;
        TTrackableVector<TRecLow> IndexLow;

        TTwoLevelLogoBlobSstIndex(const TMemoryConsumer& consumer, const TTrackableVector<TRec>& linearIndex)
            : IndexHigh(BuildLogoBlobSstIndexHigh(consumer, linearIndex))
            , IndexLow(TMemoryConsumer(consumer))
        {
            IndexLow.reserve(linearIndex.size());
            for (const TRec& rec : linearIndex) {
                IndexLow.emplace_back(rec.GetKey().LogoBlobID(), rec.GetMemRec());
            }
        }

        ui64 Elements() const {
            return IndexLow.size();
        }

        TKeyLogoBlob FirstKey() const {
            return MakeKey(IndexHigh.front(), IndexLow.front());
        }

        TKeyLogoBlob LastKey() const {
            return MakeKey(IndexHigh.back(), IndexLow.back());
        }

        static TKeyLogoBlob MakeKey(const TRecHigh& high, const TRecLow& low) {
            return TKeyLogoBlob(TLogoBlobID(high.TabletId, high.ChannelGeneration << 24 | low.Step >> 8, low.Raw2));
        }

        void Save(TTrackableVector<TRec>* linearIndex) const {
            if (IndexLow.empty()) {
                return;
            }

            linearIndex->clear();
            linearIndex->reserve(IndexLow.size());

            const TRecHigh* high = IndexHigh.begin();
            const TRecLow* low = IndexLow.begin();
            const TRecLow* lowRangeEnd = low + high->LowRangeEndIndex;

            while (low != IndexLow.end()) {
                linearIndex->emplace_back(MakeKey(*high, *low), low->GetMemRec());

                ++low;
                if (Y_UNLIKELY(low == lowRangeEnd)) {
                    ++high;
                    if (high != IndexHigh.end()) {
                        lowRangeEnd = IndexLow.begin() + high->LowRangeEndIndex;
                    }
                }
            }
        }

        class TIterator;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TTwoLevelLogoBlobSstIndex::TIterator
    ////////////////////////////////////////////////////////////////////////////
    class TTwoLevelLogoBlobSstIndex::TIterator {
        const TTwoLevelLogoBlobSstIndex* Index = nullptr;

        const TRecHigh* High = nullptr;
        const TRecLow* Low = nullptr;
        const TRecLow* LowRangeBegin = nullptr;

    public:
        TIterator() = default;

        TIterator(const TTwoLevelLogoBlobSstIndex* index)
            : Index(index)
        {}

        bool Valid() const {
            return Index && Low && Low >= Index->IndexLow.begin() && Low < Index->IndexLow.end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Low;
            if (Y_UNLIKELY(Low == Index->IndexLow.begin() + High->LowRangeEndIndex)) {
                ++High;
                LowRangeBegin = Low;
            }
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Index && Low
                    && Low >= Index->IndexLow.begin() && Low <= Index->IndexLow.end());

            if (Y_UNLIKELY(Low == LowRangeBegin)) {
                --High;
                LowRangeBegin = Index->IndexLow.begin() +
                        (High <= Index->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex);
            }
            --Low;
        }

        TKeyLogoBlob GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return MakeKey(*High, *Low);
        }

        TMemRecLogoBlob GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Low->GetMemRec();
        }

        void SeekToFirst() {
            High = Index->IndexHigh.begin();
            Low = LowRangeBegin = Index->IndexLow.begin();
        }

        void SeekToLast() {
            High = Index->IndexHigh.end();
            Low = LowRangeBegin = Index->IndexLow.end();
            Prev();
        }

        void Seek(const TKeyLogoBlob& key) {
            const TRecHigh keyHigh(key.LogoBlobID());
            const TRecLow keyLow(key.LogoBlobID());

            High = std::lower_bound(Index->IndexHigh.begin(), Index->IndexHigh.end(), keyHigh);

            if (High == Index->IndexHigh.end()) {
                Low = LowRangeBegin = Index->IndexLow.end();
                return;
            }

            auto rangeBegin = Index->IndexLow.begin() +
                    (High == Index->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex);

            if (!High->SameKey(keyHigh)) {
                Low = LowRangeBegin = rangeBegin;
                return;
            }

            auto rangeEnd = Index->IndexLow.begin() + High->LowRangeEndIndex;

            Low = std::lower_bound(rangeBegin, rangeEnd, keyLow);

            if (Low == rangeEnd) {
                LowRangeBegin = rangeEnd;
                ++High;
            } else {
                LowRangeBegin = rangeBegin;
            }
        }

        bool operator == (const TIterator& it) const {
            return High == it.High && Low == it.Low;
        }
    };
} // NKikimr
