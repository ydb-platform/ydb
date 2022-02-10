#include "gd_entry.h"
#include "gd_stats.h"

#include <util/generic/algorithm.h>
#include <util/generic/singleton.h>

namespace NGreedyDict {
    class TAlphas {
        char Memory[512];

    public:
        TStringBufs Alphas;

        TAlphas() {
            for (ui32 i = 0; i < 256; ++i) {
                Memory[2 * i] = (char)i;
                Memory[2 * i + 1] = 0;

                Alphas.push_back(TStringBuf(&Memory[2 * i], 1));
            }
        }
    };

    void TEntrySet::InitWithAlpha() {
        Pool.ClearKeepFirstChunk();
        const TStringBufs& a = Singleton<TAlphas>()->Alphas;
        for (auto it : a) {
            Add(it);
        }
        BuildHierarchy();
    }

    void TEntrySet::BuildHierarchy() {
        Sort(begin(), end(), TEntry::StrLess);

        TCompactTrieBuilder<char, ui32, TAsIsPacker<ui32>> builder(CTBF_PREFIX_GROUPED);

        for (iterator it = begin(); it != end(); ++it) {
            it->Number = (it - begin());
            TStringBuf suff = it->Str;
            size_t len = 0;
            ui32 val = 0;

            if (builder.FindLongestPrefix(suff.data(), suff.size(), &len, &val) && len) {
                it->NearestPrefix = val;
            }

            builder.Add(suff.data(), suff.size(), it->Number);
        }

        TBufferOutput bout;
        builder.Save(bout);
        Trie.Init(TBlob::FromBuffer(bout.Buffer()));
    }

    TEntry* TEntrySet::FindPrefix(TStringBuf& str) {
        size_t len = 0;
        ui32 off = 0;

        if (!Trie.FindLongestPrefix(str, &len, &off)) {
            return nullptr;
        }

        str.Skip(len);
        return &Get(off);
    }

    void TEntrySet::SetModelP() {
        for (iterator it = begin(); it != end(); ++it) {
            TEntry& e = *it;

            if (!e.HasPrefix()) {
                e.ModelP = 0;
                continue;
            }

            TStringBuf suff = e.Str;
            const TEntry& p = Get(e.NearestPrefix);
            suff.Skip(p.Len());

            float modelp = float(p.Count + e.Count) / TotalCount;

            while (!!suff) {
                TEntry* pp = FindPrefix(suff);
                modelp *= float(pp->Count + e.Count) / TotalCount;
            }

            e.ModelP = modelp;
        }
    }

    void TEntrySet::SetScores(EEntryScore s) {
        for (auto& it : *this) {
            it.Score = Score(s, it.Len(), it.ModelP, it.Count, TotalCount);
        }
    }

}
