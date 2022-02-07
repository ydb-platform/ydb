#pragma once

#include "gd_stats.h"

#include <library/cpp/containers/comptrie/comptrie.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <util/memory/pool.h>

namespace NGreedyDict {
    using TStringBufs = TVector<TStringBuf>;

    struct TEntry {
        static const i32 NoPrefix = -1;

        TStringBuf Str;

        i32 NearestPrefix = NoPrefix;
        ui32 Count = 0;
        ui32 Number = 0;
        float ModelP = 0;
        float Score = 0;

        TEntry(TStringBuf b = TStringBuf(), ui32 cnt = 0)
            : Str(b)
            , Count(cnt)
        {
        }

        bool HasPrefix() const {
            return NearestPrefix != NoPrefix;
        }
        ui32 Len() const {
            return Str.size();
        }

        static bool StrLess(const TEntry& a, const TEntry& b) {
            return a.Str < b.Str;
        }
        static bool NumberLess(const TEntry& a, const TEntry& b) {
            return a.Number < b.Number;
        }
        static bool ScoreMore(const TEntry& a, const TEntry& b) {
            return a.Score > b.Score;
        }
    };

    class TEntrySet: public TVector<TEntry>, TNonCopyable {
        TMemoryPool Pool{8112};
        TCompactTrie<char, ui32, TAsIsPacker<ui32>> Trie;

    public:
        ui32 TotalCount = 0;

        void InitWithAlpha();

        void Add(TStringBuf a) {
            push_back(TStringBuf(Pool.Append(a.data(), a.size()), a.size()));
        }

        void Add(TStringBuf a, TStringBuf b) {
            size_t sz = a.size() + b.size();
            char* p = (char*)Pool.Allocate(sz);
            memcpy(p, a.data(), a.size());
            memcpy(p + a.size(), b.data(), b.size());
            push_back(TStringBuf(p, sz));
        }

        TEntry& Get(ui32 idx) {
            return (*this)[idx];
        }

        const TEntry& Get(ui32 idx) const {
            return (*this)[idx];
        }

        void BuildHierarchy();

        // longest prefix
        TEntry* FindPrefix(TStringBuf& str);

        const TEntry* FindPrefix(TStringBuf& str) const {
            return ((TEntrySet*)this)->FindPrefix(str);
        }

        const TEntry* FirstPrefix(const TEntry& e, TStringBuf& suff) {
            if (!e.HasPrefix())
                return nullptr;

            const TEntry& p = Get(e.NearestPrefix);
            suff = e.Str;
            suff.Skip(p.Str.size());
            return &p;
        }

        void SetModelP();
        void SetScores(EEntryScore);
    };

}
