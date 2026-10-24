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
    // TLogoBlobSstIndex::TIterator: the iterator of whichever form the index holds
    ////////////////////////////////////////////////////////////////////////////
    class TLogoBlobSstIndex::TIterator {
        using TIterators = std::variant<
            TTwoLevelLogoBlobSstIndex::TIterator,
            TPackedLogoBlobSstIndex::TIterator>;

        TIterators It; // default: an iterator over no index, i.e. invalid

        // Not std::visit: libc++ dispatches through a function-pointer table that the compiler does not always fold
        // away (4x slower scans in a benchmark). Not std::get either: the variant is never valueless (no iterator
        // constructor throws), and with Y_ASSUME saying so the compiler tests the stored index once, with no throw path.
        template <class TFunc>
        decltype(auto) Visit(TFunc&& f) const {
            static_assert(std::variant_size_v<TIterators> == 2);
            Y_ASSUME(std::get_if<0>(&It) || std::get_if<1>(&It));
            if (const auto* it = std::get_if<0>(&It)) {
                return f(*it);
            }
            return f(*std::get_if<1>(&It));
        }

        template <class TFunc>
        decltype(auto) Visit(TFunc&& f) {
            static_assert(std::variant_size_v<TIterators> == 2);
            Y_ASSUME(std::get_if<0>(&It) || std::get_if<1>(&It));
            if (auto* it = std::get_if<0>(&It)) {
                return f(*it);
            }
            return f(*std::get_if<1>(&It));
        }

    public:
        TIterator() = default;

        TIterator(const TLogoBlobSstIndex* index)
            : It(std::visit([](const auto& form) { return TIterators(&form); }, index->Form))
        {}

        bool Valid() const {
            return Visit([](const auto& it) { return it.Valid(); });
        }

        void Next() {
            Visit([](auto& it) { it.Next(); });
        }

        void Prev() {
            Visit([](auto& it) { it.Prev(); });
        }

        TKeyLogoBlob GetCurKey() const {
            return Visit([](const auto& it) { return it.GetCurKey(); });
        }

        TMemRecLogoBlob GetMemRec() const {
            return Visit([](const auto& it) { return it.GetMemRec(); });
        }

        void SeekToFirst() {
            Visit([](auto& it) { it.SeekToFirst(); });
        }

        void SeekToLast() {
            Visit([](auto& it) { it.SeekToLast(); });
        }

        void Seek(const TKeyLogoBlob& key) {
            Visit([&](auto& it) { it.Seek(key); });
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

        TMemIterator(const TLevelSegment *segment)
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

        const TDiskPart* GetOutbound() const {
            return Segment->GetOutbound();
        }
    };

} // NKikimr
