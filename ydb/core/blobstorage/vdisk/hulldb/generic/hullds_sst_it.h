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

        typedef ptrdiff_t difference_type;
        typedef TRec value_type;
        typedef const TRec * pointer;
        typedef const TRec & reference;

        typedef std::bidirectional_iterator_tag iterator_category;


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

        const TRec &operator*() const {
            return *Ptr;
        }

        const TRec *operator->() const {
            return Ptr;
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            merger->AddFromSegment(Ptr->MemRec, Segment->GetOutbound(), GetCurKey(), Segment->Info.LastLsn);
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

    ////////////////////////////////////////////////////////////////////////////
    // TLevelSegment methods
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    const TKey &TLevelSegment<TKey, TMemRec>::FirstKey() const {
        TMemIterator it(this);
        it.SeekToFirst();
        return it->Key;
    }

    template <class TKey, class TMemRec>
    const TKey &TLevelSegment<TKey, TMemRec>::LastKey() const {
        TMemIterator it(this);
        it.SeekToLast();
        return it->Key;
    }

} // NKikimr
