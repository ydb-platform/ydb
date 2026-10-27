#pragma once

#include "hullds_sst.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TFlatSstIndex::TIterator
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFlatSstIndex<TKey, TMemRec>::TIterator {
        const TFlatSstIndex *Index = nullptr;
        const TRec *Ptr = nullptr;

        const TRec *Begin() const {
            return Index->Recs.data();
        }

        const TRec *End() const {
            return Index->Recs.data() + Index->Recs.size();
        }

    public:
        TIterator() = default;

        TIterator(const TFlatSstIndex *index)
            : Index(index)
        {}

        bool Valid() const {
            return Ptr && Ptr >= Begin() && Ptr < End();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Ptr;
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Ptr && Ptr <= End() && Ptr >= Begin());
            --Ptr;
        }

        TKey GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Ptr->GetKey();
        }

        TMemRec GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Ptr->GetMemRec();
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
            Ptr = ::LowerBound(Begin(), End(), key);
        }

        bool operator == (const TIterator &it) const {
            return Ptr == it.Ptr;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLogoBlobSstIndex::TIterator
    ////////////////////////////////////////////////////////////////////////////
    class TLogoBlobSstIndex::TIterator {
        TTwoLevelLogoBlobSstIndex::TIterator It; // default: an iterator over no index, i.e. invalid

    public:
        TIterator() = default;

        TIterator(const TLogoBlobSstIndex* index)
            : It(&index->Form)
        {}

        bool Valid() const {
            return It.Valid();
        }

        void Next() {
            It.Next();
        }

        void Prev() {
            It.Prev();
        }

        TKeyLogoBlob GetCurKey() const {
            return It.GetCurKey();
        }

        TMemRecLogoBlob GetMemRec() const {
            return It.GetMemRec();
        }

        void SeekToFirst() {
            It.SeekToFirst();
        }

        void SeekToLast() {
            It.SeekToLast();
        }

        void Seek(const TKeyLogoBlob& key) {
            It.Seek(key);
        }

        bool operator == (const TIterator& it) const {
            return It == it.It;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSegment::TMemIterator
    //
    // Value-type iterator over one SST: the index iterator plus what needs the segment (outbound, lsn)
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegment<TKey, TMemRec>::TMemIterator {
    protected:
        const TLevelSegment *Segment = nullptr;
        typename TIndex::TIterator It; // default: invalid

    public:
        TMemIterator() = default;

        explicit TMemIterator(const TLevelSegment *segment)
            : Segment(segment)
            , It(&segment->Index)
        {}

        bool Valid() const {
            return It.Valid();
        }

        void Next() {
            It.Next();
        }

        void Prev() {
            It.Prev();
        }

        TKey GetCurKey() const {
            return It.GetCurKey();
        }

        TMemRec GetMemRec() const {
            return It.GetMemRec();
        }

        void SeekToFirst() {
            It.SeekToFirst();
        }

        void SeekToLast() {
            It.SeekToLast();
        }

        void Seek(const TKey& key) {
            It.Seek(key);
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
            return It == it.It;
        }

        TDiskDataExtractor* GetDiskData(TDiskDataExtractor* extr) const {
            return GetMemRec().GetDiskData(extr, Segment->GetOutbound());
        }

        const TLevelSegment* GetSstPtr() const {
            return Segment;
        }
    };

} // NKikimr
