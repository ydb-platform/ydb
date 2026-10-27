#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TTwoLevelLogoBlobSstIndex -- the LogoBlob SST index in two uncoded levels
    //
    // TRecHigh per group of records with the same TabletId, Channel, Generation and Step >> 8.
    // TRecLow per record with the rest of the key and the memrec.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TTwoLevelLogoBlobSstIndex {
        using TRec = TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>;

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

            TLogoBlobIdLow() {
                Raw.X = 0;
            }

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
        private:
            TLogoBlobIdHigh Key;
            ui32 LowRangeEndIndex;

        public:
            TRecHigh(TLogoBlobIdHigh key)
                : Key(key)
            {}

            TLogoBlobIdHigh GetKey() const {
                return ReadUnaligned<TLogoBlobIdHigh>(&Key);
            }

            ui32 GetLowRangeEndIndex() const {
                return LowRangeEndIndex;
            }

            void SetLowRangeEndIndex(ui32 i) {
                LowRangeEndIndex = i;
            }

            bool operator < (const TLogoBlobIdHigh &key) const {
                return GetKey() < key;
            }
        };

        static_assert(sizeof(TRecHigh) == 20, "expect sizeof(TRecHigh) == 20");

        struct TRecLow {
        private:
            TLogoBlobIdLow Key;
            TMemRecLogoBlob MemRec;

        public:
            TRecLow(TLogoBlobIdLow key, TMemRecLogoBlob memRec)
                : Key(key)
                , MemRec(memRec)
            {}

            TLogoBlobIdLow GetKey() const {
                return ReadUnaligned<TLogoBlobIdLow>(&Key);
            }

            TMemRecLogoBlob GetMemRec() const {
                return ReadUnaligned<TMemRecLogoBlob>(&MemRec);
            }

            bool operator < (const TLogoBlobIdLow &key) const {
                return GetKey() < key;
            }
        };

        static_assert(sizeof(TRecLow) == 28, "expect sizeof(TRecLow) == 28");
#pragma pack(pop)

        TTrackableVector<TRecHigh> IndexHigh;
        TTrackableVector<TRecLow> IndexLow;

        TTwoLevelLogoBlobSstIndex(const TMemoryConsumer& consumer, const TTrackableVector<TRec>& linearIndex)
            : IndexHigh(TMemoryConsumer(consumer))
            , IndexLow(TMemoryConsumer(consumer))
        {
            if (linearIndex.empty()) {
                return;
            }

            IndexHigh.reserve(linearIndex.size());
            IndexLow.reserve(linearIndex.size());

            const TRec* rec = linearIndex.begin();

            auto blobId = rec->GetKey().LogoBlobID();
            TLogoBlobIdHigh high(blobId);
            TLogoBlobIdLow low(blobId);
            TLogoBlobIdHigh highPrev = high;

            IndexHigh.emplace_back(high);
            IndexLow.emplace_back(low, rec->GetMemRec());
            ++rec;

            for (; rec != linearIndex.end(); ++rec) {
                auto blobId = rec->GetKey().LogoBlobID();
                TLogoBlobIdHigh high(blobId);
                TLogoBlobIdLow low(blobId);

                if (Y_UNLIKELY(high != highPrev)) {
                    IndexHigh.back().SetLowRangeEndIndex(IndexLow.size());
                    IndexHigh.emplace_back(high);
                    highPrev = high;
                }

                IndexLow.emplace_back(low, rec->GetMemRec());
            }

            IndexHigh.back().SetLowRangeEndIndex(IndexLow.size());
            IndexHigh.shrink_to_fit();
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
            const auto highKey = high.GetKey();
            const auto lowKey = low.GetKey();
            return TKeyLogoBlob(TLogoBlobID(highKey.Raw.X[0], highKey.Raw.X[1], lowKey.Raw.X));
        }

        void Save(TTrackableVector<TRec>* linearIndex) const {
            linearIndex->clear();
            linearIndex->reserve(IndexLow.size());
            ui64 i = 0;
            for (const TRecHigh& high : IndexHigh) {
                for (; i < high.GetLowRangeEndIndex(); ++i) {
                    linearIndex->emplace_back(MakeKey(high, IndexLow[i]), IndexLow[i].GetMemRec());
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
            if (Y_UNLIKELY(Low == Index->IndexLow.begin() + High->GetLowRangeEndIndex())) {
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
                        (High <= Index->IndexHigh.begin() ? 0 : (High - 1)->GetLowRangeEndIndex());
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
            const TLogoBlobIdHigh keyHigh(key.LogoBlobID());
            const TLogoBlobIdLow keyLow(key.LogoBlobID());

            High = std::lower_bound(Index->IndexHigh.begin(), Index->IndexHigh.end(), keyHigh);

            if (High == Index->IndexHigh.end()) {
                Low = LowRangeBegin = Index->IndexLow.end();
                return;
            }

            auto rangeBegin = Index->IndexLow.begin() +
                    (High == Index->IndexHigh.begin() ? 0 : (High - 1)->GetLowRangeEndIndex());

            if (High->GetKey() != keyHigh) {
                Low = LowRangeBegin = rangeBegin;
                return;
            }

            auto rangeEnd = Index->IndexLow.begin() + High->GetLowRangeEndIndex();

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
