#pragma once

#include "flat_mem_iter.h"
#include "flat_part_iter.h"
#include "flat_range_cache.h"

namespace NKikimr {
namespace NTable {

struct TTableIterOps {
    static inline int CompareKeys(
            TArrayRef<const NScheme::TTypeInfoOrder> types,
            TArrayRef<const TCell> a,
            TArrayRef<const TCell> b) noexcept
    {
        if (int cmp = CompareTypedCellVectors(a.data(), b.data(), types.data(), Min(a.size(), b.size()))) {
            return cmp;
        }

        if (a.size() != b.size()) {
            // smaller key is filled with +inf => always bigger
            return a.size() < b.size() ? +1 : -1;
        }

        return 0;
    }

    static inline void MoveNext(TMemIter& it) {
        it.Next();
    }

    static inline EReady MoveNext(TRunIter& it) {
        return it.Next();
    }

    static inline void Seek(TMemIter& it, TArrayRef<const TCell> key, ESeek seek) {
        it.Seek(key, seek);
    }

    static inline EReady Seek(TRunIter& it, TArrayRef<const TCell> key, ESeek seek) {
        return it.Seek(key, seek);
    }

    class TEraseCacheOpsCommon {
    protected:
        TKeyRangeCache* Cache;

    public:
        TEraseCacheOpsCommon(TKeyRangeCache* cache = nullptr)
            : Cache(cache)
        { }

        inline explicit operator bool() const {
            return Cache != nullptr;
        }

        inline void Touch(TKeyRangeCache::const_iterator it) {
            Cache->Touch(it);
        }

        inline void EvictOld() {
            Cache->EvictOld();
        }

        inline TArrayRef<TCell> AllocateKey(TArrayRef<const TCell> key) {
            return Cache->AllocateKey(key);
        }

        inline void DeallocateKey(TArrayRef<TCell> key) {
            Cache->DeallocateKey(key);
        }

        inline bool CachingAllowed() {
            return Cache->CachingAllowed();
        }

        inline size_t MinRows() {
            return Cache->GetConfig().MinRows;
        }
    };

    class TEraseCacheOps : public TEraseCacheOpsCommon {
    public:
        using TEraseCacheOpsCommon::TEraseCacheOpsCommon;

        inline bool CrossedAtLeft(
                TKeyRangeCache::const_iterator it,
                TArrayRef<const TCell> key)
        {
            return !IsEnd(it) && Cache->InsideLeft(it, key);
        }

        inline bool CrossedAtRight(
            TKeyRangeCache::const_iterator it,
            TArrayRef<const TCell> key)
        {
            return !IsEnd(it) && Cache->InsideRight(it, key);
        }

        inline TKeyRangeCache::const_iterator MergeAdjacent(
                TKeyRangeCache::const_iterator prev,
                TKeyRangeCache::const_iterator next,
                TRowVersion version)
        {
            return Cache->Merge(prev, next, version);
        }

        inline void ExtendForward(
                TKeyRangeCache::const_iterator it,
                TArrayRef<TCell> key,
                bool inclusive,
                TRowVersion version)
        {
            Cache->ExtendRight(it, key, inclusive, version);
        }

        inline void ExtendBackward(
                TKeyRangeCache::const_iterator it,
                TArrayRef<TCell> key,
                bool inclusive,
                TRowVersion version)
        {
            Cache->ExtendLeft(it, key, inclusive, version);
        }

        inline void AddRange(
                TArrayRef<TCell> first,
                TArrayRef<TCell> last,
                TRowVersion version)
        {
            Cache->Add(TKeyRangeEntry(first, last, true, true, version));
        }

        inline std::pair<TKeyRangeCache::const_iterator, bool> FindKey(
                TArrayRef<const TCell> key)
        {
            return Cache->FindKey(key);
        }

        inline TKeyRangeCache::const_iterator Next(TKeyRangeCache::const_iterator it) {
            ++it;
            return it;
        }

        inline bool IsEnd(TKeyRangeCache::const_iterator it) {
            return it == Cache->end();
        }

        static inline TArrayRef<const TCell> EndKey(TKeyRangeCache::const_iterator it) {
            return it->ToKey;
        }

        static inline bool EndInclusive(TKeyRangeCache::const_iterator it) {
            return it->ToInclusive;
        }
    };
};

struct TTableItReverseOps {
    static int CompareKeys(
            TArrayRef<const NScheme::TTypeInfoOrder> types,
            TArrayRef<const TCell> a,
            TArrayRef<const TCell> b) noexcept
    {
        return -TTableIterOps::CompareKeys(types, a, b);
    }

    static void MoveNext(TMemIter& it) {
        it.Prev();
    }

    static EReady MoveNext(TRunIter& it) {
        return it.Prev();
    }

    static void Seek(TMemIter& it, TArrayRef<const TCell> key, ESeek seek) {
        it.SeekReverse(key, seek);
    }

    static EReady Seek(TRunIter& it, TArrayRef<const TCell> key, ESeek seek) {
        return it.SeekReverse(key, seek);
    }

    class TEraseCacheOps : public TTableIterOps::TEraseCacheOpsCommon {
    public:
        using TEraseCacheOpsCommon::TEraseCacheOpsCommon;

        inline bool CrossedAtLeft(
                TKeyRangeCache::const_iterator it,
                TArrayRef<const TCell> key)
        {
            return !IsEnd(it) && Cache->InsideRight(it, key);
        }

        inline bool CrossedAtRight(
            TKeyRangeCache::const_iterator it,
            TArrayRef<const TCell> key)
        {
            return !IsEnd(it) && Cache->InsideLeft(it, key);
        }

        inline TKeyRangeCache::const_iterator MergeAdjacent(
                TKeyRangeCache::const_iterator prev,
                TKeyRangeCache::const_iterator next,
                TRowVersion version)
        {
            return Cache->Merge(next, prev, version);
        }

        inline void ExtendForward(
                TKeyRangeCache::const_iterator it,
                TArrayRef<TCell> key,
                bool inclusive,
                TRowVersion version)
        {
            Cache->ExtendLeft(it, key, inclusive, version);
        }

        inline void ExtendBackward(
                TKeyRangeCache::const_iterator it,
                TArrayRef<TCell> key,
                bool inclusive,
                TRowVersion version)
        {
            Cache->ExtendRight(it, key, inclusive, version);
        }

        inline void AddRange(
                TArrayRef<TCell> first,
                TArrayRef<TCell> last,
                TRowVersion version)
        {
            Cache->Add(TKeyRangeEntry(last, first, true, true, version));
        }

        inline std::pair<TKeyRangeCache::const_iterator, bool> FindKey(
                TArrayRef<const TCell> key)
        {
            return Cache->FindKeyReverse(key);
        }

        inline TKeyRangeCache::const_iterator Next(TKeyRangeCache::const_iterator it) {
            if (it == Cache->begin()) {
                return Cache->end();
            }
            --it;
            return it;
        }

        inline bool IsEnd(TKeyRangeCache::const_iterator it) {
            return it == Cache->end();
        }

        static inline TArrayRef<const TCell> EndKey(TKeyRangeCache::const_iterator it) {
            return it->FromKey;
        }

        static inline bool EndInclusive(TKeyRangeCache::const_iterator it) {
            return it->FromInclusive;
        }
    };
};

}
}
