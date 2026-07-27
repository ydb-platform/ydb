#include <ydb/core/blobstorage/vdisk/query/query_statalgo.h>

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>
#include <util/stream/null.h>

#include <algorithm>

namespace NKikimr {
namespace {

    constexpr bool IsVerbose = false;
#define Ctest (IsVerbose ? Cerr : Cnull)

    using TKey = TKeyLogoBlob;
    using TMemRec = TMemRecLogoBlob;
    using TLogoBlobsLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
    using TLogoBlobsLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
    using TYieldedState = TDbStatYeildedState<TKey, TMemRec>;

    TKey MakeKey(ui32 step) {
        return TKey(TLogoBlobID(1, 1, step, 0, 0, 0));
    }

    class TManualMonotonicTimeProviderForTraverse : public NMonotonic::IMonotonicTimeProvider {
    public:
        TMonotonic Now() override {
            return CurrentTime;
        }

        void Advance(TDuration duration) {
            CurrentTime += duration;
        }

    private:
        TMonotonic CurrentTime = TMonotonic::Zero();
    };

    struct TSstSpec {
        ui64 Id = 0;
        TVector<TKey> Keys;
    };

    enum class EMutation {
        AddFreshKey,
        AddSst,
        RemoveSst,
        AddKeyToSst,
        RemoveKeyFromSst,
        Count,
    };

    class TRandomizedLogoBlobsBase {
    public:
        TRandomizedLogoBlobsBase()
            : Arena(std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate))
            , Index(MakeIntrusive<TLogoBlobsLevelIndex>(Contexts.GetLevelIndexSettings(), Arena))
        {
            Index->CurSlice->SortedLevels.emplace_back(TKey());
            Index->LoadCompleted();

            for (ui32 step = 1'000; step < 1'200; step += 10) {
                PutFresh(MakeKey(step), true);
            }
            for (ui32 sstIdx = 0; sstIdx < 6; ++sstIdx) {
                Level0.push_back(MakeSstSpec(2'000 + sstIdx * 100, ++NextLevel0Id));
            }
            for (ui32 sstIdx = 0; sstIdx < 8; ++sstIdx) {
                SortedLevel.push_back(MakeSstSpec(4'000 + sstIdx * 100));
            }
            RegisterInitialSstKeys(Level0);
            RegisterInitialSstKeys(SortedLevel);
            RebuildSsts();
        }

        TIntrusivePtr<THullCtx> GetHullCtx() {
            return Contexts.GetHullCtx();
        }

        TLevelIndexSnapshot<TKey, TMemRec> GetSnapshot() {
            return Index->GetIndexSnapshot();
        }

        const TSet<TKey>& GetExpectedKeys() const {
            return ExpectedKeys;
        }

        void Mutate(const TYieldedState& yieldedState, TReallyFastRng32& rng) {
            const EMutation mutation = static_cast<EMutation>(
                rng() % static_cast<ui32>(EMutation::Count));
            switch (mutation) {
                case EMutation::AddFreshKey:
                    AddFreshKey(yieldedState, rng);
                    break;
                case EMutation::AddSst:
                    AddSst(yieldedState, rng);
                    break;
                case EMutation::RemoveSst:
                    RemoveSst(rng);
                    break;
                case EMutation::AddKeyToSst:
                    AddKeyToSst(rng);
                    break;
                case EMutation::RemoveKeyFromSst:
                    RemoveKeyFromSst(rng);
                    break;
                case EMutation::Count:
                    Y_ABORT("unexpected mutation");
            }
            RebuildSsts();
        }

        void PrintStatistics(ui32 seed, ui32 quanta, ui32 yields, size_t seenKeys) const {
            Ctest << "TraverseDbWithoutMerge randomized test statistics:"
                << " seed# " << seed
                << " quanta# " << quanta
                << " yields# " << yields
                << " seenKeys# " << seenKeys
                << " expectedKeys# " << ExpectedKeys.size()
                << " finalLevel0Ssts# " << Level0.size()
                << " finalSortedSsts# " << SortedLevel.size()
                << " freshKeysAdded# " << FreshKeysAdded
                << " sstsAdded# " << SstsAdded
                << " sstsRemoved# " << SstsRemoved
                << " sstKeysAdded# " << SstKeysAdded
                << " sstKeysRemoved# " << SstKeysRemoved
                << Endl;
        }

        void AssertAllMutationKindsWereUsed() const {
            UNIT_ASSERT_C(FreshKeysAdded, "no fresh keys were added");
            UNIT_ASSERT_C(SstsAdded, "no SSTs were added");
            UNIT_ASSERT_C(SstsRemoved, "no SSTs were removed");
            UNIT_ASSERT_C(SstKeysAdded, "no keys were added to SSTs");
            UNIT_ASSERT_C(SstKeysRemoved, "no keys were removed from SSTs");
        }

    private:
        static TSstSpec MakeSstSpec(ui32 firstStep, ui64 id = 0) {
            TSstSpec spec{.Id = id};
            for (ui32 offset = 0; offset < 60; offset += 10) {
                spec.Keys.push_back(MakeKey(firstStep + offset));
            }
            return spec;
        }

        void RegisterInitialSstKeys(const TVector<TSstSpec>& specs) {
            for (const TSstSpec& spec : specs) {
                for (const TKey& key : spec.Keys) {
                    UNIT_ASSERT(AllKeys.insert(key).second);
                    ExpectedKeys.insert(key);
                }
            }
        }

        TIntrusivePtr<TLogoBlobsLevelSegment> MakeSst(const TSstSpec& spec) {
            TTrackableVector<TLogoBlobsLevelSegment::TRec> records(
                TMemoryConsumer(Contexts.GetVCtx()->SstIndex));
            for (const TKey& key : spec.Keys) {
                records.emplace_back(key, TMemRec());
            }

            auto sst = MakeIntrusive<TLogoBlobsLevelSegment>(Contexts.GetVCtx());
            sst->LoadLinearIndex(records);
            sst->VolatileOrderId = spec.Id;
            return sst;
        }

        void RebuildSsts() {
            auto& level0 = *Index->CurSlice->Level0.Segs;
            level0.Segments.clear();
            level0.Num = 0;
            Sort(Level0, [](const TSstSpec& lhs, const TSstSpec& rhs) {
                return lhs.Id < rhs.Id;
            });
            for (const TSstSpec& spec : Level0) {
                Index->CurSlice->Level0.Put(MakeSst(spec));
            }

            auto& sorted = Index->CurSlice->SortedLevels.front().Segs->Segments;
            sorted.clear();
            Sort(SortedLevel, [](const TSstSpec& lhs, const TSstSpec& rhs) {
                return lhs.Keys.front() < rhs.Keys.front();
            });
            for (const TSstSpec& spec : SortedLevel) {
                auto sst = MakeSst(spec);
                Index->CurSlice->SortedLevels.front().Put(sst);
            }
        }

        void PutFresh(const TKey& key, bool expected) {
            UNIT_ASSERT_C(AllKeys.insert(key).second, "duplicate generated key");
            Index->PutToFresh(NextLsn++, key, TMemRec());
            if (expected) {
                ExpectedKeys.insert(key);
            }
        }

        void AddFreshKey(const TYieldedState& yieldedState, TReallyFastRng32& rng) {
            const auto* freshPosition =
                std::get_if<typename TYieldedState::TFreshPosition>(&yieldedState.Position);
            ui32 step = NextLateFreshStep++;
            bool expected = false;
            if (freshPosition) {
                const ui32 resumeStep = freshPosition->Key.LogoBlobID().Step();
                const bool addAfterResumePosition = rng() % 2;
                step = addAfterResumePosition ? resumeStep + 1 : resumeStep - 1;
                while (AllKeys.contains(MakeKey(step))) {
                    step += addAfterResumePosition ? 1 : -1;
                }
                expected = !(MakeKey(step) < freshPosition->Key);
            }
            PutFresh(MakeKey(step), expected);
            ++FreshKeysAdded;
        }

        void AddSst(const TYieldedState& yieldedState, TReallyFastRng32& rng) {
            const bool addToLevel0 = rng() % 2;
            TSstSpec spec;
            if (addToLevel0) {
                spec = MakeSstSpec(NextLevel0Step, ++NextLevel0Id);
                NextLevel0Step += 100;
            } else {
                spec = MakeSstSpec(NextSortedStep);
                NextSortedStep += 100;
            }

            bool expected = std::holds_alternative<typename TYieldedState::TFreshPosition>(
                yieldedState.Position);
            if (const auto* levelPosition =
                    std::get_if<typename TYieldedState::TLevelPosition>(&yieldedState.Position)) {
                if (addToLevel0) {
                    expected = levelPosition->Level == 0 &&
                        spec.Id > std::get<typename TYieldedState::TLevelPosition::
                            TUnsortedLevelDiscriminator>(levelPosition->Discriminator);
                } else if (levelPosition->Level == 0) {
                    expected = true;
                } else {
                    expected = std::get<typename TYieldedState::TLevelPosition::
                        TSortedLevelDiscriminator>(levelPosition->Discriminator) <
                        spec.Keys.front();
                }
            }

            for (const TKey& key : spec.Keys) {
                UNIT_ASSERT_C(AllKeys.insert(key).second, "duplicate generated key");
                if (expected) {
                    ExpectedKeys.insert(key);
                }
            }
            (addToLevel0 ? Level0 : SortedLevel).push_back(std::move(spec));
            ++SstsAdded;
        }

        bool IsFullySeen(const TSstSpec& spec) const {
            return std::all_of(spec.Keys.begin(), spec.Keys.end(), [this](const TKey& key) {
                return SeenKeys && SeenKeys->contains(key);
            });
        }

        bool IsFullyPending(const TSstSpec& spec) const {
            return std::all_of(spec.Keys.begin(), spec.Keys.end(), [this](const TKey& key) {
                return ExpectedKeys.contains(key) && (!SeenKeys || !SeenKeys->contains(key));
            });
        }

        TVector<std::pair<TVector<TSstSpec>*, size_t>> GetMutableSstCandidates() {
            TVector<std::pair<TVector<TSstSpec>*, size_t>> candidates;
            auto collect = [this, &candidates](TVector<TSstSpec>& specs) {
                for (size_t idx = 0; idx < specs.size(); ++idx) {
                    if (IsFullySeen(specs[idx]) || IsFullyPending(specs[idx])) {
                        candidates.emplace_back(&specs, idx);
                    }
                }
            };
            collect(Level0);
            collect(SortedLevel);
            return candidates;
        }

        void RemoveSst(TReallyFastRng32& rng) {
            auto candidates = GetMutableSstCandidates();
            if (candidates.empty()) {
                return;
            }
            auto [specs, idx] = candidates[rng() % candidates.size()];
            if (IsFullyPending((*specs)[idx])) {
                for (const TKey& key : (*specs)[idx].Keys) {
                    ExpectedKeys.erase(key);
                }
            }
            specs->erase(specs->begin() + idx);
            ++SstsRemoved;
        }

        void AddKeyToSst(TReallyFastRng32& rng) {
            auto candidates = GetMutableSstCandidates();
            if (candidates.empty()) {
                return;
            }
            auto [specs, idx] = candidates[rng() % candidates.size()];
            TSstSpec& spec = (*specs)[idx];
            const bool pending = IsFullyPending(spec);

            size_t gapIdx = rng() % (spec.Keys.size() - 1);
            ui32 step = spec.Keys[gapIdx].LogoBlobID().Step() + 1;
            while (AllKeys.contains(MakeKey(step))) {
                ++step;
            }
            UNIT_ASSERT_C(MakeKey(step) < spec.Keys[gapIdx + 1], "SST key gap exhausted");

            const TKey key = MakeKey(step);
            UNIT_ASSERT(AllKeys.insert(key).second);
            spec.Keys.push_back(key);
            Sort(spec.Keys);
            if (pending) {
                ExpectedKeys.insert(key);
            }
            ++SstKeysAdded;
        }

        void RemoveKeyFromSst(TReallyFastRng32& rng) {
            auto candidates = GetMutableSstCandidates();
            candidates.erase(std::remove_if(candidates.begin(), candidates.end(), [](const auto& candidate) {
                return (*candidate.first)[candidate.second].Keys.size() < 2;
            }), candidates.end());
            if (candidates.empty()) {
                return;
            }
            auto [specs, idx] = candidates[rng() % candidates.size()];
            TSstSpec& spec = (*specs)[idx];
            const size_t keyIdx = 1 + rng() % (spec.Keys.size() - 1);
            const TKey key = spec.Keys[keyIdx];
            if (!SeenKeys || !SeenKeys->contains(key)) {
                ExpectedKeys.erase(key);
            }
            spec.Keys.erase(spec.Keys.begin() + keyIdx);
            ++SstKeysRemoved;
        }

    public:
        void SetSeenKeys(const TSet<TKey>& seenKeys) {
            SeenKeys = &seenKeys;
        }

    private:
        TTestContexts Contexts;
        std::shared_ptr<TRopeArena> Arena;
        TIntrusivePtr<TLogoBlobsLevelIndex> Index;
        TVector<TSstSpec> Level0;
        TVector<TSstSpec> SortedLevel;
        TSet<TKey> AllKeys;
        TSet<TKey> ExpectedKeys;
        const TSet<TKey>* SeenKeys = nullptr;
        ui64 NextLsn = 1;
        ui64 NextLevel0Id = 0;
        ui32 NextLateFreshStep = 1'500;
        ui32 NextLevel0Step = 10'000;
        ui32 NextSortedStep = 20'000;
        ui32 FreshKeysAdded = 0;
        ui32 SstsAdded = 0;
        ui32 SstsRemoved = 0;
        ui32 SstKeysAdded = 0;
        ui32 SstKeysRemoved = 0;
    };

    struct TCollectingAggregator {
        TSet<TKey>& SeenKeys;
        TManualMonotonicTimeProviderForTraverse& TimeProvider;
        bool Finished = false;

        void UpdateFresh(const char*, const TKey& key, const TMemRec&) {
            Update(key);
        }

        void UpdateLevel(const TLogoBlobsLevelSegment::TLevelSstPtr&, const TKey& key, const TMemRec&) {
            Update(key);
        }

        void Finish() {
            Finished = true;
        }

    private:
        void Update(const TKey& key) {
            UNIT_ASSERT_C(SeenKeys.insert(key).second,
                TStringBuilder() << "duplicate key during resumed traversal: " << key.ToString());
            TimeProvider.Advance(TDuration::MilliSeconds(1));
        }
    };

    Y_UNIT_TEST_SUITE(TTraverseDbWithoutMergeTest) {
        Y_UNIT_TEST(RandomizedMutationDuringTraversal) {
            constexpr ui32 Seed = 0x51a7e;
            constexpr ui32 MaxQuanta = 500;
            TRandomizedLogoBlobsBase database;
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProviderForTraverse>();
            TSet<TKey> seenKeys;
            database.SetSeenKeys(seenKeys);
            TCollectingAggregator aggregator{seenKeys, *timeProvider};
            std::optional<TYieldedState> yieldedState;
            TReallyFastRng32 rng(Seed);
            ui32 quanta = 0;
            ui32 yields = 0;

            for (; quanta < MaxQuanta;) {
                ++quanta;
                auto snapshot = database.GetSnapshot();
                yieldedState = TraverseDbWithoutMerge(
                    database.GetHullCtx(),
                    &aggregator,
                    snapshot,
                    std::move(yieldedState),
                    TDbStatYieldPolicy{
                        .StepsBeforeMeasures = 4,
                        .QuantDuration = TDuration::MilliSeconds(3),
                        .DelayBetweenQuants = TDuration::Zero(),
                    },
                    timeProvider);
                snapshot.Destroy();

                if (!yieldedState) {
                    break;
                }

                ++yields;
                if (yields <= 80) {
                    database.Mutate(*yieldedState, rng);
                }
            }

            database.PrintStatistics(Seed, quanta, yields, seenKeys.size());
            UNIT_ASSERT_C(!yieldedState, "traversal did not finish");
            UNIT_ASSERT_C(aggregator.Finished, "aggregator was not finished");
            UNIT_ASSERT_C(yields > 10, "traversal did not yield often enough");
            UNIT_ASSERT(seenKeys == database.GetExpectedKeys());
            database.AssertAllMutationKindsWereUsed();
        }
    }

#undef Ctest

} // anonymous namespace
} // NKikimr
