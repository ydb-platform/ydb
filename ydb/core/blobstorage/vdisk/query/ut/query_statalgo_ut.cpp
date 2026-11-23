#include <ydb/core/blobstorage/vdisk/query/query_statalgo.h>

#include "query_stat_test_utils.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    using TKey = TKeyLogoBlob;
    using TMemRec = TMemRecLogoBlob;
    using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
    using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
    using TYieldedState = TDbStatYieldedState<TKey, TMemRec>;
    using NDbStatTest::TManualMonotonicTimeProvider;

    TKey MakeKey(ui32 step) {
        return TKey(TLogoBlobID(1, 1, step, 0, 0, 0));
    }

    class TTestDatabase {
    public:
        TTestDatabase()
            : Arena(std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate))
            , Index(MakeIntrusive<TLevelIndex>(Contexts.GetLevelIndexSettings(), Arena))
        {
            Index->CurSlice->SortedLevels.emplace_back(TKey());
            Index->LoadCompleted();

            PutFresh(10);
            PutFresh(20);
            PutFresh(20); // two physical fresh records for the same key
            PutFresh(30);
            AddLevel0Sst({15, 20, 40}); // and another physical record for key 20
        }

        TIntrusivePtr<THullCtx> GetHullCtx() {
            return Contexts.GetHullCtx();
        }

        TLevelIndexSnapshot<TKey, TMemRec> GetSnapshot() {
            return Index->GetIndexSnapshot();
        }

        void PutFresh(ui32 step) {
            Index->PutToFresh(NextLsn++, MakeKey(step), MakeMemRec());
        }

    private:
        TMemRec MakeMemRec() {
            return TMemRec(TIngress(NextRecordId++));
        }

        void AddLevel0Sst(std::initializer_list<ui32> steps) {
            TTrackableVector<TLevelSegment::TRec> records(TMemoryConsumer(Contexts.GetVCtx()->SstIndex));
            for (ui32 step : steps) {
                records.emplace_back(MakeKey(step), MakeMemRec());
            }

            auto sst = MakeIntrusive<TLevelSegment>(Contexts.GetVCtx());
            sst->LoadLinearIndex(records);
            sst->VolatileOrderId = 1;
            Index->CurSlice->Level0.Put(sst);
        }

    private:
        TTestContexts Contexts;
        std::shared_ptr<TRopeArena> Arena;
        TIntrusivePtr<TLevelIndex> Index;
        ui64 NextLsn = 1;
        ui32 NextRecordId = 1;
    };

    struct TCollectingAggregator {
        TVector<ui32>& SeenSteps;
        THashMap<ui32, TVector<ui32>>& QuantaByStep;
        TManualMonotonicTimeProvider& TimeProvider;
        ui32 CurrentQuantum = 0;
        bool Finished = false;
        TVector<std::pair<ui32, ui64>>* SeenRecords = nullptr;

        void Update(const TKey& key, const TMemRec& memRec) {
            const ui32 step = key.LogoBlobID().Step();
            SeenSteps.push_back(step);
            QuantaByStep[step].push_back(CurrentQuantum);
            if (SeenRecords) {
                SeenRecords->emplace_back(step, memRec.GetIngress().Raw());
            }
            TimeProvider.Advance(TDuration::MilliSeconds(1));
        }

        void UpdateFresh(const char*, const TKey& key, const TMemRec& memRec) {
            Update(key, memRec);
        }

        void UpdateLevel(const TLevelSegment::TLevelSstPtr&, const TKey& key, const TMemRec& memRec) {
            Update(key, memRec);
        }

        void BeginKey(const TKey&) {
        }

        void UpdateFreshRecord(const TMemRec& memRec, const TRope*, const TKey& key, ui64) {
            Update(key, memRec);
        }

        void UpdateLevelRecord(const TMemRec& memRec, const TDiskPart*, const TKey& key, ui64,
                const TLevelSegment*) {
            Update(key, memRec);
        }

        void FinishKey(const TKey&) {
        }

        void Finish() {
            Finished = true;
        }
    };

    Y_UNIT_TEST_SUITE(TTraverseDbWithoutMergeYieldTest) {
        Y_UNIT_TEST(YieldingAndNonYieldingProcessSameRecords) {
            TTestDatabase database;

            auto nonYieldingTimeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TVector<ui32> nonYieldingSteps;
            THashMap<ui32, TVector<ui32>> nonYieldingQuanta;
            TVector<std::pair<ui32, ui64>> nonYieldingRecords;
            TCollectingAggregator nonYieldingAggregator{
                nonYieldingSteps,
                nonYieldingQuanta,
                *nonYieldingTimeProvider,
                0,
                false,
                &nonYieldingRecords,
            };
            auto snapshot = database.GetSnapshot();
            TraverseDbWithoutMerge(database.GetHullCtx(), &nonYieldingAggregator, snapshot);
            snapshot.Destroy();

            auto yieldingTimeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TVector<ui32> yieldingSteps;
            THashMap<ui32, TVector<ui32>> yieldingQuanta;
            TVector<std::pair<ui32, ui64>> yieldingRecords;
            TCollectingAggregator yieldingAggregator{
                yieldingSteps,
                yieldingQuanta,
                *yieldingTimeProvider,
                0,
                false,
                &yieldingRecords,
            };
            std::optional<TYieldedState> yieldedState;
            for (ui32 quantum = 0; quantum < 10; ++quantum) {
                yieldingAggregator.CurrentQuantum = quantum;
                auto yieldingSnapshot = database.GetSnapshot();
                yieldedState = TraverseDbWithoutMerge(
                    database.GetHullCtx(),
                    &yieldingAggregator,
                    yieldingSnapshot,
                    std::move(yieldedState),
                    TDbStatYieldPolicy{
                        .StepsBeforeMeasures = 1,
                        .QuantumDuration = TDuration::Zero(),
                        .DelayBetweenQuanta = TDuration::Zero(),
                    },
                    yieldingTimeProvider);
                yieldingSnapshot.Destroy();
                if (!yieldedState) {
                    break;
                }
            }

            Sort(nonYieldingSteps);
            Sort(yieldingSteps);
            Sort(nonYieldingRecords);
            Sort(yieldingRecords);
            UNIT_ASSERT(!yieldedState);
            UNIT_ASSERT(nonYieldingAggregator.Finished);
            UNIT_ASSERT(yieldingAggregator.Finished);
            UNIT_ASSERT(nonYieldingSteps == yieldingSteps);
            UNIT_ASSERT(nonYieldingRecords == yieldingRecords);
            UNIT_ASSERT_VALUES_EQUAL(nonYieldingRecords.size(), 7);
            UNIT_ASSERT(nonYieldingSteps == TVector<ui32>({10, 15, 20, 20, 20, 30, 40}));
        }

        Y_UNIT_TEST(ReverseTraversalYieldsOnlyBetweenCompleteKeys) {
            TTestDatabase database;
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TVector<ui32> seenSteps;
            THashMap<ui32, TVector<ui32>> quantaByStep;
            TCollectingAggregator aggregator{seenSteps, quantaByStep, *timeProvider};
            std::optional<TYieldedState> yieldedState;

            for (ui32 quantum = 0; quantum < 10; ++quantum) {
                aggregator.CurrentQuantum = quantum;
                auto snapshot = database.GetSnapshot();
                yieldedState = TraverseDbWithoutMerge(
                    database.GetHullCtx(),
                    &aggregator,
                    snapshot,
                    std::move(yieldedState),
                    TDbStatYieldPolicy{
                        .StepsBeforeMeasures = 1,
                        .QuantumDuration = TDuration::Zero(),
                        .DelayBetweenQuanta = TDuration::Zero(),
                    },
                    timeProvider);
                snapshot.Destroy();
                if (!yieldedState) {
                    break;
                }
            }

            UNIT_ASSERT(!yieldedState);
            UNIT_ASSERT(aggregator.Finished);
            UNIT_ASSERT(seenSteps == TVector<ui32>({40, 30, 20, 20, 20, 15, 10}));
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[20].size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[20].front(), quantaByStep[20].back());
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[40].front(), 0);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[30].front(), 1);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[20].front(), 2);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[15].front(), 3);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[10].front(), 4);
        }

        Y_UNIT_TEST(ReacquiredSnapshotContinuesBelowLastCompletedKey) {
            TTestDatabase database;
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TVector<ui32> seenSteps;
            THashMap<ui32, TVector<ui32>> quantaByStep;
            TCollectingAggregator aggregator{seenSteps, quantaByStep, *timeProvider};
            std::optional<TYieldedState> yieldedState;

            auto runQuantum = [&] {
                auto snapshot = database.GetSnapshot();
                yieldedState = TraverseDbWithoutMerge(
                    database.GetHullCtx(),
                    &aggregator,
                    snapshot,
                    std::move(yieldedState),
                    TDbStatYieldPolicy{
                        .StepsBeforeMeasures = 1,
                        .QuantumDuration = TDuration::Zero(),
                        .DelayBetweenQuanta = TDuration::Zero(),
                    },
                    timeProvider);
                snapshot.Destroy();
                ++aggregator.CurrentQuantum;
            };

            runQuantum();
            UNIT_ASSERT_VALUES_EQUAL(seenSteps.back(), 40);

            // A lower new key is still pending; a higher one is beyond the
            // completed reverse-traversal boundary and must not extend the job.
            database.PutFresh(35);
            database.PutFresh(45);
            for (ui32 quantum = 0; yieldedState && quantum < 10; ++quantum) {
                runQuantum();
            }

            UNIT_ASSERT(!yieldedState);
            UNIT_ASSERT(aggregator.Finished);
            UNIT_ASSERT(seenSteps == TVector<ui32>({40, 35, 30, 20, 20, 20, 15, 10}));
        }

        Y_UNIT_TEST(StopPredicateReturnsAfterCompleteKeyAndResumeHasNoDuplicates) {
            TTestDatabase database;
            auto timeProvider = MakeIntrusive<TManualMonotonicTimeProvider>();
            TVector<ui32> seenSteps;
            THashMap<ui32, TVector<ui32>> quantaByStep;
            TVector<std::pair<ui32, ui64>> seenRecords;
            TCollectingAggregator aggregator{
                seenSteps,
                quantaByStep,
                *timeProvider,
                0,
                false,
                &seenRecords,
            };

            auto snapshot = database.GetSnapshot();
            std::optional<TYieldedState> yieldedState = TraverseDbWithoutMergeUntil(
                database.GetHullCtx(),
                &aggregator,
                snapshot,
                std::optional<TYieldedState>{},
                std::nullopt,
                [&] {
                    // The predicate is consulted only after all physical
                    // records for the current key have been processed. The
                    // third distinct key (step 20) has three such records.
                    return seenSteps.back() == 20;
                },
                timeProvider);
            snapshot.Destroy();

            UNIT_ASSERT(yieldedState);
            UNIT_ASSERT_VALUES_EQUAL(yieldedState->LastProcessedKey.LogoBlobID().Step(), 20);
            UNIT_ASSERT(!aggregator.Finished);
            UNIT_ASSERT(seenSteps == TVector<ui32>({40, 30, 20, 20, 20}));
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[20].size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(quantaByStep[20].front(), quantaByStep[20].back());

            aggregator.CurrentQuantum = 1;
            auto resumedSnapshot = database.GetSnapshot();
            yieldedState = TraverseDbWithoutMergeUntil(
                database.GetHullCtx(),
                &aggregator,
                resumedSnapshot,
                std::move(yieldedState),
                std::nullopt,
                [] { return false; },
                timeProvider);
            resumedSnapshot.Destroy();

            UNIT_ASSERT(!yieldedState);
            UNIT_ASSERT(aggregator.Finished);
            UNIT_ASSERT(seenSteps == TVector<ui32>({40, 30, 20, 20, 20, 15, 10}));
            UNIT_ASSERT_VALUES_EQUAL(seenRecords.size(), 7);

            THashSet<ui64> recordIds;
            for (const auto& [step, recordId] : seenRecords) {
                Y_UNUSED(step);
                UNIT_ASSERT(recordIds.insert(recordId).second);
            }
            UNIT_ASSERT_VALUES_EQUAL(recordIds.size(), seenRecords.size());

            // A disabled time policy never consults the clock; the split was
            // caused exclusively by the explicit stop predicate.
            UNIT_ASSERT_VALUES_EQUAL(timeProvider->GetCalls(), 0);
        }
    }

} // anonymous namespace
} // namespace NKikimr
