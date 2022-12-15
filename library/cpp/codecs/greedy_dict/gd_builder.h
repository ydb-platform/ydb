#pragma once

#include "gd_entry.h"

#include <util/generic/hash.h>
#include <util/random/fast.h>

namespace NGreedyDict {
    struct TBuildSettings {
        EEntryStatTest StatTest = EST_SIMPLE_NORM;
        EEntryScore Score = ES_LEN_SIMPLE;

        float MinPValue = 0.75;
        ui32 MinAbsCount = 10;
        ui32 GrowLimit = 10; // times of maxentries
        bool Verbose = false;
    };

    class TDictBuilder {
        using TCompoundCounts = THashMap<ui64, ui32, THash<ui64>, TEqualTo<ui64>, TPoolAllocator>;
        using TCandidate = std::pair<float, ui64>;
        using TCandidates = TVector<TCandidate>;

    private:
        TFastRng64 Rng{0x1a5d0ac170565c1c, 0x0be7bc27, 0x6235f6f57820aa0d, 0xafdc7fb};
        TStringBufs Input;

        THolder<TEntrySet> Current;

        TMemoryPool CompoundCountsPool;
        THolder<TCompoundCounts> CompoundCounts;

        TCandidates Candidates;

        TBuildSettings Settings;

    public:
        TDictBuilder(const TBuildSettings& s = TBuildSettings())
            : CompoundCountsPool(8112, TMemoryPool::TLinearGrow::Instance())
            , Settings(s)
        {
        }

        void SetInput(const TStringBufs& in) {
            Input = in;
        }

        const TBuildSettings& GetSettings() const {
            return Settings;
        }

        TBuildSettings& GetSettings() {
            return Settings;
        }

        void SetSettings(const TBuildSettings& s) {
            Settings = s;
        }

        TEntrySet& EntrySet() {
            return *Current;
        }

        const TEntrySet& EntrySet() const {
            return *Current;
        }

        THolder<TEntrySet> ReleaseEntrySet() {
            return std::move(Current);
        }

        ui32 /*iters*/ Build(ui32 maxentries, ui32 maxiters = 16, ui32 maxlen = -1, ui32 mindiff = 10);

    public:
        void RebuildCounts(ui32 maxcand, bool final);
        ui32 /*diff size*/ BuildNextGeneration(ui32 maxent, ui32 maxlen);

        static bool IsCompound(ui64 ent) {
            return ent & 0xFFFFFFFF00000000ULL;
        }

        static ui32 Next(ui64 ent) {
            return ent;
        }
        static ui32 Prev(ui64 ent) {
            return (ent >> 32) - 1;
        }

        static ui64 Compose(ui32 prev, ui32 next) {
            return ((prev + 1ULL) << 32) | next;
        }
    };

}
