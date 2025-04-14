#pragma once

#include "hullds_sst.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSegment::TMemIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TMemIterator {
    protected:
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef typename TLevelSegment::TRec TRec;

        const TLevelSegment *Segment;
        const TRec *Ptr;

        const TRec *Begin() const {
            return Segment->LoadedIndex.data();
        }

        const TRec *End() const {
            return Segment->LoadedIndex.data() + Segment->LoadedIndex.size();
        }

        TRec *Begin() {
            return const_cast<TLevelSegment *>(Segment)->LoadedIndex.data();
        }

        TRec *End() {
            return const_cast<TLevelSegment *>(Segment)->LoadedIndex.data() + Segment->LoadedIndex.size();
        }

    public:
        TMemIterator(const TLevelSegment *segment)
            : Segment(segment)
            , Ptr(nullptr)
        {}

        TMemIterator()
            : Segment(nullptr)
            , Ptr(nullptr)
        {}

        TMemIterator(const TMemIterator &i) {
            Segment = i.Segment;
            Ptr = i.Ptr;
        }

        TMemIterator &operator=(const TMemIterator &i) {
            Segment = i.Segment;
            Ptr = i.Ptr;
            return *this;
        }

        bool Valid() const {
            return Ptr && Ptr >= Begin() && Ptr < End();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Ptr && Ptr < End() && Ptr >= Begin());
            ++Ptr;
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Ptr && Ptr <= End() && Ptr >= Begin());
            --Ptr;
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Ptr->Key;
        }

        const TMemRec& GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Ptr->MemRec;
        }

        void SeekToFirst() {
            Ptr = Begin();
        }

        void SeekToLast() {
            Ptr = End();
            --Ptr;
        }

        void Seek(const TKey& key) {
            // Advance to the first entry with a key >= target
            Ptr = ::LowerBound(Begin(), End(), key, typename TRec::TLess());
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            merger->AddFromSegment(Ptr->MemRec, Segment->GetOutbound(), GetCurKey(), Segment->Info.LastLsn, Segment);
        }

        template <class Heap>
        void PutToHeap(Heap& heap) {
            heap.Add(this);
        }

        bool operator == (const TMemIterator &it) const {
            Y_ABORT_UNLESS(Segment == it.Segment);
            return Ptr == it.Ptr;
        }

        bool operator != (const TMemIterator &it) const {
            return !operator == (it);
        }

        TDiskDataExtractor *GetDiskData(TDiskDataExtractor *extr) const {
            return Ptr->MemRec.GetDiskData(extr, Segment->GetOutbound());
        }

        const TLevelSegment *GetSstPtr() const {
            return Segment;
        }

        const TDiskPart *GetOutbound() const {
            return Segment->GetOutbound();
        }
    };

    template <>
    class TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TMemIterator {
    protected:
        typedef ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob> TLevelSegment;

        const TLevelSegment* Segment = nullptr;

        const TLevelSegment::TRecHigh* High = nullptr;
        const TLevelSegment::TRecLow* Low = nullptr;
        const TLevelSegment::TRecLow* LowRangeBegin = nullptr;

    public:
        TMemIterator(const TLevelSegment* segment)
            : Segment(segment)
        {}

        TMemIterator() = default;

        TMemIterator(const TMemIterator& i) {
            Segment = i.Segment;
            High = i.High;
            Low = i.Low;
            LowRangeBegin = i.LowRangeBegin;
        }

        TMemIterator& operator=(const TMemIterator& i) {
            Segment = i.Segment;
            High = i.High;
            Low = i.Low;
            LowRangeBegin = i.LowRangeBegin;
            return *this;
        }

        bool Valid() const {
            return Segment && Low && Low >= Segment->IndexLow.begin() && Low < Segment->IndexLow.end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Low;
            if (Y_UNLIKELY(Low == Segment->IndexLow.begin() + High->LowRangeEndIndex)) {
                ++High;
                LowRangeBegin = Low;
            }
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Segment && Low
                    && Low >= Segment->IndexLow.begin() && Low <= Segment->IndexLow.end());

            if (Y_UNLIKELY(Low == LowRangeBegin)) {
                --High;
                LowRangeBegin = Segment->IndexLow.begin() +
                        (High <= Segment->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex);
            }
            --Low;
        }

        TKeyLogoBlob GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            const auto& high = High->Key;
            const auto& low = Low->Key;
            return TKeyLogoBlob(TLogoBlobID(high.Raw.X[0], high.Raw.X[1], low.Raw.X));
        }

        const TMemRecLogoBlob& GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Low->MemRec;
        }

        void SeekToFirst() {
            High = Segment->IndexHigh.begin();
            Low = LowRangeBegin = Segment->IndexLow.begin();
        }

        void SeekToLast() {
            High = Segment->IndexHigh.end();
            Low = LowRangeBegin = Segment->IndexLow.end();
            Prev();
        }

        void Seek(const TKeyLogoBlob& key) {
            TLevelSegment::TLogoBlobIdHigh keyHigh(key.LogoBlobID());
            TLevelSegment::TLogoBlobIdLow keyLow(key.LogoBlobID());

            High = std::lower_bound(Segment->IndexHigh.begin(), Segment->IndexHigh.end(),
                    keyHigh, TLevelSegment::TRecHigh::TLess());

            if (High == Segment->IndexHigh.end()) {
                Low = LowRangeBegin = Segment->IndexLow.end();
                return;
            }

            auto rangeBegin = Segment->IndexLow.begin() +
                    (High == Segment->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex);

            if (High->Key != keyHigh) {
                Low = LowRangeBegin = rangeBegin;
                return;
            }

            auto rangeEnd = Segment->IndexLow.begin() + High->LowRangeEndIndex;

            Low = std::lower_bound(rangeBegin, rangeEnd, keyLow, TLevelSegment::TRecLow::TLess());

            if (Low == rangeEnd) {
                LowRangeBegin = rangeEnd;
                ++High;
            } else {
                LowRangeBegin = rangeBegin;
            }
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger* merger) {
            merger->AddFromSegment(GetMemRec(), Segment->GetOutbound(), GetCurKey(), Segment->Info.LastLsn, Segment);
        }

        template <class Heap>
        void PutToHeap(Heap& heap) {
            heap.Add(this);
        }

        bool operator == (const TMemIterator& it) const {
            Y_ABORT_UNLESS(Segment == it.Segment);
            return High == it.High && Low == it.Low;
        }

        bool operator != (const TMemIterator& it) const {
            return !(operator == (it));
        }

        TDiskDataExtractor* GetDiskData(TDiskDataExtractor* extr) const {
            return GetMemRec().GetDiskData(extr, Segment->GetOutbound());
        }

        const TLevelSegment* GetSstPtr() const {
            return Segment;
        }

        const TDiskPart* GetOutbound() const {
            return Segment->GetOutbound();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSegment methods
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    TKey TLevelSegment<TKey, TMemRec>::FirstKey() const {
        TMemIterator it(this);
        it.SeekToFirst();
        return it.GetCurKey();
    }

    template <class TKey, class TMemRec>
    TKey TLevelSegment<TKey, TMemRec>::LastKey() const {
        TMemIterator it(this);
        it.SeekToLast();
        return it.GetCurKey();
    }

} // NKikimr
