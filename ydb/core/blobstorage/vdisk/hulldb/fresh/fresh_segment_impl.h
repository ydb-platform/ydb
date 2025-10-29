#pragma once

#include "defs.h"
#include "fresh_segment.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <library/cpp/threading/skip_list/skiplist.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/system/align.h>
#include <util/generic/set.h>


namespace NKikimr {
    namespace NFreshSegment {

        /////////////////////////////////////////////////////////////////////////////////////////
        // NFreshSegment::TIdxKey
        /////////////////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        struct TIdxKey {
            ui64 Lsn;
            TKey Key;
            TMemRec MemRec;

            TIdxKey()
                : Lsn(0)
                , Key()
                , MemRec()
            {}

            TIdxKey(ui64 lsn, const TKey &key, const TMemRec &memRec)
                : Lsn(lsn)
                , Key(key)
                , MemRec(memRec)
            {}

            TIdxKey(const TIdxKey &x)
                : Lsn(x.Lsn)
                , Key(x.Key)
                , MemRec(x.MemRec)
            {}

            struct TLess {
                i64 operator () (const TIdxKey &x, const TIdxKey &y) const {
                    if (x.Key < y.Key)
                        return -1;
                    else if (y.Key < x.Key)
                        return 1;
                    else {
                        i64 res = (x.Lsn < y.Lsn) ? -1 : ((y.Lsn < x.Lsn) ? 1 : 0);
                        return res;
                    }
                }
            };

            TString ToString() const {
                TStringStream str;
                str << "[Key# " << Key.ToString() << " Lsn# " << Lsn << "]";
                return str.Str();
            }

            void DebugPrint(IOutputStream &str) const {
                str << "key=" << Key.ToString() << " lsn=" << Lsn;
            }
        };

    } // NFreshSegment
} // NKikimr


template <class TKey, class TMemRec>
struct TPodTraits<::NKikimr::NFreshSegment::TIdxKey<TKey, TMemRec>> {
    enum {
        IsPod = TTypeTraits<TKey>::IsPod && TTypeTraits<TMemRec>::IsPod
    };
};


namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndex
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndex {
    public:
        using TIdxKey = NFreshSegment::TIdxKey<TKey, TMemRec>;
        using TSkipList = NThreading::TSkipList<TIdxKey, typename TIdxKey::TLess, TTransparentMemoryPool>;

        class TIterator;

        TFreshIndex(TVDiskContextPtr vctx)
            : LocalArena(TMemoryConsumer(vctx->FreshIndex), (8 << 20), TMemoryPool::TLinearGrow::Instance())
            , Idx(LocalArena)
        {}

        bool Insert(const TIdxKey &key) {
            return Idx.Insert(key);
        }

    private:
        TTransparentMemoryPool LocalArena;
        TSkipList Idx;
    };


    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndex::TIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndex<TKey, TMemRec>::TIterator {
    public:
        using TSkipListIndex = typename ::NKikimr::TFreshIndex<TKey, TMemRec>;
        using TSkipListIterator = typename TSkipList::TIterator;

        TIterator() = default;
        ~TIterator() = default;
        TIterator(const TIterator &) = default;
        TIterator &operator=(const TIterator &) = default;
        TIterator(TIterator &&) = default;
        TIterator &operator=(TIterator &&) = default;

        TIterator(const TSkipListIndex *skipListIndex)
            : SkipListIndex(skipListIndex)
        {}

        bool Valid() const {
            return It.IsValid();
        }
        void Next() {
            It.Next();
        }
        void Prev() {
            It.Prev();
        }
        void SeekToFirst() {
            Y_DEBUG_ABORT_UNLESS(SkipListIndex);
            It = SkipListIndex->Idx.SeekToFirst();
        }
        void SeekToLast() {
            Y_DEBUG_ABORT_UNLESS(SkipListIndex);
            It = SkipListIndex->Idx.SeekToLast();
        }
        void SeekTo(const TIdxKey &key) {
            Y_DEBUG_ABORT_UNLESS(SkipListIndex);
            It = SkipListIndex->Idx.SeekTo(key);
        }

        const TIdxKey& GetValue() const {
            Y_DEBUG_ABORT_UNLESS(It.IsValid());
            return It.GetValue();
        }

    private:
        const TSkipListIndex *SkipListIndex = nullptr;
        TSkipListIterator It;
    };





    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData implementation
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    TFreshIndexAndData<TKey, TMemRec>::TFreshIndexAndData(
            THullCtxPtr hullCtx,
            std::shared_ptr<TRopeArena> arena)
        : TBase(TDeleteInBatchPool(hullCtx->VCtx->ActorSystem))
        , HullCtx(hullCtx)
        , Index(new TFreshIndex(hullCtx->VCtx))
        , FreshDataMemConsumer(hullCtx->VCtx->FreshData)
        , Arena(std::move(arena))
    {}

    template <class TKey, class TMemRec>
    void TFreshIndexAndData<TKey, TMemRec>::PutPrepared(ui64 lsn, const TKey &key, const TMemRec &memRec) {
        typename TFreshIndex::TIdxKey idxKey(lsn, key, memRec);
        Index->Insert(idxKey); // every key is unique, thanks to lsn

        Inserts++;
        Y_DEBUG_ABORT_UNLESS(lsn != ui64(-1) && (LastLsn <= lsn || LastLsn == 0));
        if (FirstLsn == ui64(-1)) {
            FirstLsn = lsn;
        }
        LastLsn = lsn;
    }

    template <typename TKey, typename TMemRec>
    inline void TFreshIndexAndData<TKey, TMemRec>::PutLogoBlobWithData(ui64 /*lsn*/, const TKey& /*key*/, ui8 /*partId*/,
            const TIngress& /*ingress*/, TRope /*buffer*/, std::optional<ui64> /*checksum*/) {
        static_assert(!std::is_same_v<TKey, TKeyLogoBlob>, "not implemented");
    }

    template <>
    inline void TFreshIndexAndData<TKeyLogoBlob, TMemRecLogoBlob>::PutLogoBlobWithData(ui64 lsn,
            const TKeyLogoBlob &key, ui8 partId, const TIngress &ingress, TRope buffer,
            std::optional<ui64> checksum) {
        TMemRecLogoBlob memRec(ingress);
        buffer = TRope::CopySpaceOptimized(std::move(buffer), 128, *Arena);
        const ui64 fullDataSize = key.LogoBlobID().BlobSize();
        const size_t delta = buffer.size();
        TRope blob = TDiskBlob::Create(fullDataSize, partId, HullCtx->VCtx->Top->GType.TotalPartCount(),
            std::move(buffer), *Arena, HullCtx->VCfg->BlobHeaderMode, checksum);
        FreshDataMemConsumer.Add(delta);
        const ui32 blobSize = blob.GetSize();

        // create additional extent if existing one has exhausted
        if (LastRopeExtentSize == RopeExtentSize) {
            RopeExtents.emplace_back();
            LastRopeExtentSize = 0;
        }

        // get the last extent and put the rope to its end
        Y_VERIFY_S(RopeExtents, HullCtx->VCtx->VDiskLogPrefix);
        auto& extent = RopeExtents.back();
        TRope& rope = extent[LastRopeExtentSize++];
        rope = std::move(blob);

        // calculate buffer id from the rope address; the address is immutable during fresh segment lifetime, so we can
        // use it directly
        uintptr_t bufferId = reinterpret_cast<uintptr_t>(&rope);
        Y_VERIFY_S((bufferId & 0x7) == 0, HullCtx->VCtx->VDiskLogPrefix);
        bufferId >>= 3;
        Y_VERIFY_S(bufferId < (ui64(1) << 62), HullCtx->VCtx->VDiskLogPrefix);
        memRec.SetMemBlob(bufferId, blobSize);

        Put(lsn, key, memRec);
    }

    template <>
    inline const TRope& TFreshIndexAndData<TKeyLogoBlob, TMemRecLogoBlob>::GetLogoBlobData(const TMemPart& memPart) const {
        const TRope& rope = *reinterpret_cast<const TRope*>(memPart.BufferId << 3);
        Y_VERIFY_S(rope.GetSize() == memPart.Size, HullCtx->VCtx->VDiskLogPrefix);
        return rope;
    }

    template <class TKey, class TMemRec>
    inline const TRope& TFreshIndexAndData<TKey, TMemRec>::GetLogoBlobData(const TMemPart& /*memPart*/) const {
        Y_ABORT("invalid call");
    }

    template <class TKey, class TMemRec>
    void TFreshIndexAndData<TKey, TMemRec>::Put(ui64 lsn, const TKey &key, const TMemRec &memRec) {
        auto type = memRec.GetType();
        auto dataSize = memRec.DataSize();
        switch (type) {
            case TBlobType::MemBlob:
                Y_VERIFY_S(dataSize, HullCtx->VCtx->VDiskLogPrefix);
                MemDataSize += AlignUp(dataSize, 8u);
                break;
            case TBlobType::DiskBlob:
                Y_VERIFY_S(!memRec.HasData(), HullCtx->VCtx->VDiskLogPrefix);
                break;
            case TBlobType::HugeBlob:
                Y_VERIFY_S(memRec.HasData(), HullCtx->VCtx->VDiskLogPrefix);
                HugeDataSize += memRec.DataSize();
                break;
            default:
                Y_ABORT("Unexpected type: type# %d", int(type));
        }
        PutPrepared(lsn, key, memRec);
    }

    template <class TKey, class TMemRec>
    void TFreshIndexAndData<TKey, TMemRec>::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
        using TIterator = typename TFreshIndex::TIterator;
        TIterator it(Index.get());
        it.SeekToFirst();
        while (it.Valid()) {
            const TMemRec &memRec = it.GetValue().MemRec;
            if (memRec.GetType() == TBlobType::HugeBlob) {
                TDiskDataExtractor extr;
                const TDiskPart& part = memRec.GetDiskData(&extr, nullptr)->SwearOne();
                if (part.Size) {
                    Y_VERIFY_S(part.ChunkIdx, HullCtx->VCtx->VDiskLogPrefix);
                    chunks.insert(part.ChunkIdx);
                }
            }
            it.Next();
        }
    }

    template <class TKey, class TMemRec>
    void TFreshIndexAndData<TKey, TMemRec>::GetHugeBlobs(TSet<TDiskPart> &hugeBlobs) const {
        using TIterator = typename TFreshIndex::TIterator;
        TIterator it(Index.get());
        it.SeekToFirst();
        while (it.Valid()) {
            const TMemRec &memRec = it.GetValue().MemRec;
            if (memRec.GetType() == TBlobType::HugeBlob) {
                TDiskDataExtractor extr;
                const TDiskPart& part = memRec.GetDiskData(&extr, nullptr)->SwearOne();
                bool inserted = hugeBlobs.insert(part).second;
                Y_VERIFY_S(inserted, HullCtx->VCtx->VDiskLogPrefix);
            }
            it.Next();
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData::TBaseIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndData<TKey, TMemRec>::TBaseIterator {
    public:
        typedef ::NKikimr::TFreshIndexAndData<TKey, TMemRec> TFreshIndexAndData;
        typedef TFreshIndexAndData TContType;
        typedef typename TFreshIndex::TIterator TIterator;

        TBaseIterator(const THullCtxPtr &hullCtx, const TContType *freshSegment, ui64 lsn)
            : Seg(freshSegment)
            , Lsn(lsn)
            , It()
        {
            Y_UNUSED(hullCtx);
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            Y_DEBUG_ABORT_UNLESS(It.Valid());
            const auto& key = It.GetValue().Key;
            bool putSomething = false;
            for (TIterator cursor = It; cursor.Valid() && cursor.GetValue().Key == key && cursor.GetValue().Lsn <= Lsn; cursor.Next()) {
                PutToMerger(cursor.GetValue().MemRec, cursor.GetValue().Lsn, merger);
                putSomething = true;
            }
            Y_DEBUG_ABORT_UNLESS(putSomething);
        }

        TKey GetCurKey() const {
            return It.GetValue().Key;
        }

        TMemRec GetUnmergedMemRec() const {
            return It.GetValue().MemRec;
        }

        // append chunk ids to the vector
        void FillInChunkIds(TVector<ui32> &vec) const {
            // fresh doesn't have chunks
            Y_UNUSED(vec);
        }

        // dump all data of the segment accessible by this iterator
        void DumpAll(IOutputStream &str) const {
            str << "=== Fresh (lsn=" << Lsn << ")===\n";
            TIterator it(Seg->Index.get());
            it.SeekToFirst();
            while (it.Valid()) {
                const auto &item = it.GetValue();
                str << item.Key.ToString() << " " << item.Lsn << "\n";
                it.Next();
            }
        }

    protected:
        const TFreshIndexAndData *Seg;
        const ui64 Lsn;
        TIterator It;

        template <class TRecordMerger>
        void PutToMerger(const TMemRec &memRec, ui64 lsn, TRecordMerger *merger) {
            TKey key = It.GetValue().Key;
            if (merger->HaveToMergeData() && memRec.GetType() == TBlobType::MemBlob) {
                const TMemPart p = memRec.GetMemData();
                const TRope& rope = Seg->GetLogoBlobData(p);
                merger->AddFromFresh(memRec, &rope, key, lsn);
            } else {
                merger->AddFromFresh(memRec, nullptr, key, lsn);
            }
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData::TForwardIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndData<TKey, TMemRec>::TForwardIterator : public TFreshIndexAndData<TKey, TMemRec>::TBaseIterator {
    public:
        typedef ::NKikimr::TFreshIndexAndData<TKey, TMemRec> TFreshIndexAndData;
        typedef typename TFreshIndexAndData::TBaseIterator TBase;
        typedef TFreshIndexAndData TContType;
        typedef typename TFreshIndex::TIterator TIterator;

    protected:
        struct TSeekCache {
            TKey SearchedKey;
            TIterator FoundIt;
            bool Initialized;

            TSeekCache()
                : SearchedKey()
                , FoundIt()
                , Initialized(false)
            {}

            bool Search(const TKey &key, TIterator &it) {
                if (Initialized && SearchedKey <= key && (!FoundIt.Valid() || key <= FoundIt.GetValue().Key)) {
                    it = FoundIt;
                    return true;
                } else {
                    return false;
                }
            }

            void Set(const TKey &key, const TIterator &it) {
                SearchedKey = key;
                FoundIt = it;
                Initialized = true;
            }
        };

    public:
        TForwardIterator(const THullCtxPtr &hullCtx, const TContType *freshSegment, ui64 lsn)
            : TBase(hullCtx, freshSegment, lsn)
        {}

        using TBase::PutToMerger;
        using TBase::GetCurKey;

        bool Valid() const {
            return It.Valid();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(It.Valid());
            const TKey& key = It.GetValue().Key;
            for (It.Next(); It.Valid() && It.GetValue().Key == key; It.Next()) {}
            for (; It.Valid() && Lsn < It.GetValue().Lsn; It.Next()) {}
        }

        void SeekToFirst() {
            if (Seg) {
                It = TIterator(Seg->Index.get());
                // just find ANY value when Lsn less or equal than required, that'll do
                for (It.SeekToFirst(); It.Valid() && Lsn < It.GetValue().Lsn; It.Next()) {}
            } else {
                It = TIterator();
            }
        }

        void Seek(const TKey &key) {
            if (!Seg) {
                It = TIterator();
            } else if (!SeekCache.Search(key, It)) {
                typename TFreshIndex::TIdxKey idxKey(0, key, TMemRec());
                It = TIterator(Seg->Index.get());
                for (It.SeekTo(idxKey); It.Valid() && Lsn < It.GetValue().Lsn; It.Next()) {}
                SeekCache.Set(key, It);
            }
        }

    protected:
        TSeekCache SeekCache;
        using TBase::It;
        using TBase::Seg;
        using TBase::Lsn;
    };


    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData::TBackwardIterator
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndData<TKey, TMemRec>::TBackwardIterator : public TFreshIndexAndData<TKey, TMemRec>::TBaseIterator {
    public:
        typedef ::NKikimr::TFreshIndexAndData<TKey, TMemRec> TFreshIndexAndData;
        typedef typename TFreshIndexAndData::TBaseIterator TBase;
        typedef TFreshIndexAndData TContType;
        typedef typename TFreshIndex::TIterator TIterator;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TContType *freshSegment, ui64 lsn)
            : TBase(hullCtx, freshSegment, lsn)
        {}

        using TBase::PutToMerger;
        using TBase::GetCurKey;

        bool Valid() const {
            return It.Valid();
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(It.Valid());
            It.Prev();
            Adjust();
        }

        void SeekToLast() {
            if (Seg) {
                It = TIterator(Seg->Index.get());
                It.SeekToLast();
                Adjust();
            } else {
                It = TIterator();
            }
        }

        void Seek(const TKey &key) {
            if (Seg) {
                typename TFreshIndex::TIdxKey idxKey(0, key, TMemRec());
                It = TIterator(Seg->Index.get());
                It.SeekTo(idxKey);
                if (!It.Valid()) {
                    SeekToLast();
                } else if (It.GetValue().Key != key) {
                    Y_DEBUG_ABORT_UNLESS(key < It.GetValue().Key);
                    Prev();
                } else {
                    Adjust();
                }
            } else {
                It = TIterator();
            }
        }

    private:
        using TBase::It;
        using TBase::Seg;
        using TBase::Lsn;

        void Adjust() {
            // move back and stop when iterator gets exhausted, or we have acceptable LSN
            for (; It.Valid() && Lsn < It.GetValue().Lsn; It.Prev()) {}
            // position to the first item of this key, if we got one
            if (It.Valid()) {
                Y_DEBUG_ABORT_UNLESS(It.GetValue().Lsn <= Lsn);
                TIterator cursor = It;
                for (cursor.Prev(); cursor.Valid() && cursor.GetValue().Key == It.GetValue().Key; cursor.Prev()) {
                    It = cursor;
                }
            }
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndDataSnapshot
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndDataSnapshot<TKey, TMemRec>::TForwardIterator : public ::NKikimr::TFreshIndexAndData<TKey, TMemRec>::TForwardIterator {
    public:
        using TBase = typename ::NKikimr::TFreshIndexAndData<TKey, TMemRec>::TForwardIterator;
        using TContType = ::NKikimr::TFreshIndexAndDataSnapshot<TKey, TMemRec>;

        TForwardIterator(const THullCtxPtr &hullCtx, const TContType *data)
            : TBase(hullCtx, (data ? data->IndexAndData.Get() : nullptr), (data ? data->SnapLsn : 0))
        {}

        template <class THeap>
        void PutToHeap(THeap& heap) {
            heap.Add(this);
        }

    };

    template <class TKey, class TMemRec>
    class TFreshIndexAndDataSnapshot<TKey, TMemRec>::TBackwardIterator : public ::NKikimr::TFreshIndexAndData<TKey, TMemRec>::TBackwardIterator {
    public:
        using TBase = typename ::NKikimr::TFreshIndexAndData<TKey, TMemRec>::TBackwardIterator;
        using TContType = ::NKikimr::TFreshIndexAndDataSnapshot<TKey, TMemRec>;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TContType *data)
            : TBase(hullCtx, (data ? data->IndexAndData.Get() : nullptr), (data ? data->SnapLsn : 0))
        {}

        template <class THeap>
        void PutToHeap(THeap& heap) {
            heap.Add(this);
        }

    };
    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshSegmentSnapshot
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshSegmentSnapshot<TKey, TMemRec>::TForwardIterator
        : public TGenericForwardIterator<
                        TKey,
                        typename TFreshIndexAndDataSnapshot::TForwardIterator,
                        typename TFreshAppendixTreeSnap::TForwardIterator>
    {
    public:
        using TBase = TGenericForwardIterator<TKey, typename TFreshIndexAndDataSnapshot::TForwardIterator,
            typename TFreshAppendixTreeSnap::TForwardIterator>;
        using TContType = TFreshSegmentSnapshot;

        TForwardIterator(const THullCtxPtr &hullCtx, const TContType *data)
            : TBase(hullCtx,
                (data ? &data->IndexAndDataSnap : nullptr),
                (data ? &data->AppendixTreeSnap : nullptr))
        {}
        TForwardIterator(const TForwardIterator &) = default;
        TForwardIterator &operator=(const TForwardIterator &) = default;

        using TBase::PutToMerger;
        using TBase::Next;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

    template <class TKey, class TMemRec>
    class TFreshSegmentSnapshot<TKey, TMemRec>::TBackwardIterator
        : public TGenericBackwardIterator<
                        TKey,
                        typename TFreshIndexAndDataSnapshot::TBackwardIterator,
                        typename TFreshAppendixTreeSnap::TBackwardIterator>
    {
    public:
        using TBase = TGenericBackwardIterator<TKey, typename TFreshIndexAndDataSnapshot::TBackwardIterator,
            typename TFreshAppendixTreeSnap::TBackwardIterator>;
        using TContType = TFreshSegmentSnapshot;

        TBackwardIterator(const THullCtxPtr &hullCtx, const TContType *data)
            : TBase(hullCtx,
                (data ? &data->IndexAndDataSnap : nullptr),
                (data ? &data->AppendixTreeSnap : nullptr))
        {}

        using TBase::PutToMerger;
        using TBase::Prev;
        using TBase::Valid;
        using TBase::Seek;
        using TBase::PutToHeap;
    };

    template <class TKey, class TMemRec>
    class TFreshSegmentSnapshot<TKey, TMemRec>::TIteratorWOMerge {
    public:
        using TContType = TFreshSegmentSnapshot;

        TIteratorWOMerge(const THullCtxPtr &hullCtx, const TContType *data)
            : It(hullCtx, data)
        {}

        TIteratorWOMerge(const TIteratorWOMerge &) = default;
        TIteratorWOMerge &operator=(const TIteratorWOMerge &) = default;

        bool Valid() const {
            return !Recs.empty();
        }

        void Next() {
            if (!Recs.empty()) {
                Recs.pop_back();
                if (!Recs.empty()) {
                    return; // more records to go with the current key
                }
            }

            if (!It.Valid()) {
                return;
            }

            struct {
                std::vector<std::pair<TKey, TMemRec>>& Recs;

                void AddFromSegment(const TMemRec&, const TDiskPart*, const TKey&, ui64, const void*) {
                    Y_DEBUG_ABORT("should not be called");
                }

                void AddFromFresh(const TMemRec& memRec, const TRope* /*data*/, const TKey& key, ui64 /*lsn*/) {
                    Recs.emplace_back(key, memRec);
                }

                static constexpr bool HaveToMergeData() { return false; }
            } m{Recs};

            It.PutToMerger(&m);
            It.Next();
        }

        void SeekToFirst() {
            Recs.clear();
            It.SeekToFirst();
            Next();
        }

        TKey GetUnmergedKey() const {
            return Recs.back().first;
        }

        TMemRec GetUnmergedMemRec() const {
            return Recs.back().second;
        }

    private:
        TForwardIterator It; // generic merging iterator
        std::vector<std::pair<TKey, TMemRec>> Recs;
    };

    template <class TKey, class TMemRec>
    void TFreshSegment<TKey, TMemRec>::OutputHtml(const TString &which, IOutputStream &str) const {
        str << "\n";
        HTML(str) {
            DIV_CLASS ("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    STRONG() { str << "Segment: " << which; }
                    str << "    StartTime: " + ToStringLocalTimeUpToSeconds(StartTime);
                }
                DIV_CLASS("panel-body") {
                    DIV_CLASS("row") {
                        OutputBasicStatHtml(str);
                    }
                    DIV_CLASS("row") {
                        COLLAPSED_BUTTON_CONTENT("appendixdetailsid", "Appendix Details") {
                            AppendixTree.OutputHtml(str);
                        }
                    }
                }
            }
        }
        str << "\n";
    }

    template <class TKey, class TMemRec>
    void TFreshSegment<TKey, TMemRec>::OutputBasicStatHtml(IOutputStream &str) const {
        auto outputFirstLsn = [&str] (ui64 firstLsn) {
            if (firstLsn == ui64(-1)) {
                str << "none";
            } else {
                str << firstLsn;
            }
        };
        HTML(str) {
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Source";}
                        TABLEH() {str << "FirstLsn";}
                        TABLEH() {str << "LastLsn";}
                        TABLEH() {str << "ItemsInserted";}
                        TABLEH() {str << "MemDataSize";}
                        TABLEH() {str << "HugeDataSize";}
                    }
                }
                // IndexAndData
                TABLEBODY() {
                    TABLER() {
                        TABLED() { str << "SkipList"; }
                        TABLED() { outputFirstLsn(IndexAndData->GetFirstLsn()); }
                        TABLED() { str << IndexAndData->GetLastLsn(); }
                        TABLED() { str << IndexAndData->ElementsInserted(); }
                        TABLED() { str << IndexAndData->GetMemDataSize(); }
                        TABLED() { str << IndexAndData->GetHugeDataSize(); }
                    }
                }
                // AppendixTree
                TABLEBODY() {
                    TABLER() {
                        TABLED() { str << "Appendix"; }
                        TABLED() { outputFirstLsn(AppendixTree.GetFirstLsn()); }
                        TABLED() { str << AppendixTree.GetLastLsn(); }
                        TABLED() { str << AppendixTree.ElementsInserted(); }
                        TABLED() { str << 0; } // don't have data
                        TABLED() { str << 0; } // don't have data
                    }
                }
            }
        }
    }

    template <class TKey, class TMemRec>
    void TFreshSegment<TKey, TMemRec>::OutputProto(NKikimrVDisk::FreshSegmentStat *stat) const {
        NKikimrVDisk::FreshIndexAndDataStat *indexAndData = stat->mutable_index_and_data();
        indexAndData->set_first_lsn(IndexAndData->GetFirstLsn());
        indexAndData->set_last_lsn(IndexAndData->GetLastLsn());
        indexAndData->set_inserted_elements(IndexAndData->ElementsInserted());
        indexAndData->set_mem_data_size(IndexAndData->GetMemDataSize());
        indexAndData->set_huge_data_size(IndexAndData->GetHugeDataSize());
        NKikimrVDisk::FreshAppendixTreeStat *appendixTree = stat->mutable_appendix_tree();
        appendixTree->set_first_lsn(AppendixTree.GetFirstLsn());
        appendixTree->set_last_lsn(AppendixTree.GetLastLsn());
        appendixTree->set_inserted_elements(AppendixTree.ElementsInserted());
    }

} // NKikimr
