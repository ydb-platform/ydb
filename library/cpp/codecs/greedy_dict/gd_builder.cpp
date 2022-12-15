#include "gd_builder.h"

#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>
#include <util/generic/algorithm.h>

#include <util/random/shuffle.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/system/rusage.h>

namespace NGreedyDict {
    void TDictBuilder::RebuildCounts(ui32 maxcand, bool final) {
        if (!Current) {
            Current = MakeHolder<TEntrySet>();
            Current->InitWithAlpha();
        }

        TEntrySet& set = *Current;

        for (auto& it : set)
            it.Count = 0;

        CompoundCounts = nullptr;
        CompoundCountsPool.Clear();

        if (!final) {
            CompoundCounts = MakeHolder<TCompoundCounts>(&CompoundCountsPool);
            CompoundCounts->reserve(maxcand);
        }

        Shuffle(Input.begin(), Input.end(), Rng);

        for (auto str : Input) {
            if (!final && CompoundCounts->size() > maxcand)
                break;

            i32 prev = -1;

            while (!!str) {
                TEntry* e = set.FindPrefix(str);
                ui32 num = e->Number;

                e->Count += 1;
                if (!final && prev >= 0) {
                    (*CompoundCounts)[Compose(prev, num)] += 1;
                }

                prev = num;
                ++set.TotalCount;
            }
        }

        Current->SetModelP();
    }

    ui32 TDictBuilder::BuildNextGeneration(ui32 maxent, ui32 maxlen) {
        TAutoPtr<TEntrySet> newset = new TEntrySet;
        newset->InitWithAlpha();
        maxent -= newset->size();

        ui32 additions = 0;
        ui32 deletions = 0;

        {
            const TEntrySet& set = *Current;

            Candidates.clear();
            const ui32 total = set.TotalCount;
            const float minpval = Settings.MinPValue;
            const EEntryStatTest test = Settings.StatTest;
            const EEntryScore score = Settings.Score;
            const ui32 mincnt = Settings.MinAbsCount;

            for (const auto& it : set) {
                const TEntry& e = it;
                float modelp = e.ModelP;
                ui32 cnt = e.Count;

                if (e.HasPrefix() && e.Count > mincnt && StatTest(test, modelp, cnt, total) > minpval)
                    Candidates.push_back(TCandidate(-Score(score, e.Len(), modelp, cnt, total), it.Number));
            }

            if (!!CompoundCounts) {
                for (TCompoundCounts::const_iterator it = CompoundCounts->begin(); it != CompoundCounts->end(); ++it) {
                    const TEntry& prev = set.Get(Prev(it->first));
                    const TEntry& next = set.Get(Next(it->first));
                    float modelp = ModelP(prev.Count, next.Count, total);
                    ui32 cnt = it->second;
                    if (cnt > mincnt && StatTest(test, modelp, cnt, total) > minpval && prev.Len() + next.Len() <= maxlen)
                        Candidates.push_back(TCandidate(-Score(score, prev.Len() + next.Len(), modelp, cnt, total), it->first));
                }
            }

            Sort(Candidates.begin(), Candidates.end());

            if (Candidates.size() > maxent)
                Candidates.resize(maxent);

            for (const auto& candidate : Candidates) {
                if (IsCompound(candidate.second)) {
                    additions++;
                    newset->Add(set.Get(Prev(candidate.second)).Str, set.Get(Next(candidate.second)).Str);
                } else {
                    newset->Add(set.Get(candidate.second).Str);
                }
            }

            deletions = set.size() - (newset->size() - additions);
        }

        Current = newset;
        Current->BuildHierarchy();
        return deletions + additions;
    }

    ui32 TDictBuilder::Build(ui32 maxentries, ui32 maxiters, ui32 maxlen, ui32 mindiff) {
        /* size_t totalsz = 0;
        for (auto it : Input)
            totalsz += it.size();*/

        while (maxiters) {
            maxiters--;

            RebuildCounts(maxentries * Settings.GrowLimit, false);

            if (Settings.Verbose) {
                TString mess = Sprintf("iter:%" PRIu32 " sz:%" PRIu32 " pend:%" PRIu32, maxiters, (ui32)Current->size(), (ui32)CompoundCounts->size());
                Clog << Sprintf("%-110s RSS=%" PRIu32 "M", mess.data(), (ui32)(TRusage::Get().MaxRss >> 20)) << Endl;
            }

            ui32 diff = BuildNextGeneration(maxentries, maxlen);

            if (Current->size() == maxentries && diff < mindiff)
                break;
        }

        RebuildCounts(0, true);
        Current->SetScores(Settings.Score);
        return maxiters;
    }

}
