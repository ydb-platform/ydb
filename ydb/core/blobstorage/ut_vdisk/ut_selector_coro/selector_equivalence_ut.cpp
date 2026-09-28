#include "selector_test_helpers.h"
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace NKikimr {
namespace {

using namespace NActors;
using namespace NHullComp;

using namespace NSelectorTest;

class TResumeCounter : public TDecorator {
    std::atomic<ui32>& Resumes;
    std::promise<TAutoPtr<IEventHandle>>* Suspended;
    std::promise<void>* Destroyed;

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
        if (ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
            Resumes.fetch_add(1, std::memory_order_relaxed);
            if (auto* suspended = std::exchange(Suspended, nullptr)) {
                suspended->set_value(TAutoPtr<IEventHandle>(ev.Release()));
                return false;
            }
        }
        return true;
    }

public:
    TResumeCounter(IActor* actor, std::atomic<ui32>& resumes, std::promise<TAutoPtr<IEventHandle>>* suspended,
            std::promise<void>* destroyed = nullptr)
        : TDecorator(THolder<IActor>(actor))
        , Resumes(resumes)
        , Suspended(suspended)
        , Destroyed(destroyed)
    {}

    ~TResumeCounter() override {
        if (Destroyed) {
            Actor.Reset();
            Destroyed->set_value();
        }
    }
};

template<class TKey, class TMemRec>
struct TSelectionResult {
    EAction Action = ActNothing;
    std::unique_ptr<TTask<TKey, TMemRec>> Task;
    std::vector<TSstRatioPtr> Ratios;
    ui32 Resumes = 0;
};

template<class TKey, class TMemRec>
class TResultReceiver : public TActor<TResultReceiver<TKey, TMemRec>> {
    using TThis = TResultReceiver<TKey, TMemRec>;
    using TResult = TSelectionResult<TKey, TMemRec>;
    std::promise<TResult> Promise;

    STFUNC(Receive) {
        TResult result;
        if (ev->GetTypeRewrite() == TSelected<TKey, TMemRec>::EventType) {
            auto* selected = ev->Get<TSelected<TKey, TMemRec>>();
            result.Action = selected->Action;
            result.Task = std::move(selected->CompactionTask);
        } else {
            // A wakeup after cancellation checks that no result preceded it.
            Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvents::TSystem::Wakeup);
        }
        Promise.set_value(std::move(result));
        this->PassAway();
    }

public:
    explicit TResultReceiver(std::promise<TResult>&& promise)
        : TActor<TThis>(&TThis::Receive)
        , Promise(std::move(promise))
    {}
};

template<class TKey, class TMemRec, template<class, class> class TSelector>
TSelectionResult<TKey, TMemRec> RunSelector(TEnvironment& env, TInput<TKey, TMemRec>& input,
        std::function<void()> whileSuspended = {}) {
    std::promise<TSelectionResult<TKey, TMemRec>> promise;
    auto future = promise.get_future();
    std::atomic<ui32> resumes = 0;
    std::promise<TAutoPtr<IEventHandle>> suspended;
    auto suspension = suspended.get_future();
    const auto recipient = env.System->Register(new TResultReceiver<TKey, TMemRec>(std::move(promise)));
    auto* selector = new TSelector<TKey, TMemRec>(input.HullCtx, input.Params,
        input.Index->GetIndexSnapshot(), input.Ds->Barriers->GetIndexSnapshot(), recipient,
        std::make_unique<TTask<TKey, TMemRec>>(), input.AllowGarbageCollection);
    env.System->Register(new TResumeCounter(selector, resumes, whileSuspended ? &suspended : nullptr), TMailboxType::HTSwap, 0);
    if (whileSuspended) {
        if (suspension.wait_for(std::chrono::seconds(60)) != std::future_status::ready) {
            env.System->Stop();
            UNIT_FAIL("selector did not suspend");
        }
        auto event = suspension.get();
        try {
            whileSuspended();
        } catch (...) {
            env.System->Stop();
            throw;
        }
        env.System->Send(event.Release());
    }
    const auto status = future.wait_for(std::chrono::seconds(60));
    if (status != std::future_status::ready) {
        // Keep the decorator's counter alive through shutdown.
        env.System->Stop();
        UNIT_FAIL("selector did not return TSelected");
    }
    auto result = future.get();
    result.Resumes = resumes.load(std::memory_order_relaxed);
    for (const auto& sst : input.Ssts) {
        result.Ratios.push_back(sst->StorageRatio.Get());
    }
    return result;
}

template<class TKey, class TMemRec>
auto SstIds(const TLeveledSsts<TKey, TMemRec>& ssts) {
    std::vector<std::pair<ui32, ui64>> result;
    typename TLeveledSsts<TKey, TMemRec>::TIterator it(&ssts);
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        const auto item = it.Get();
        result.emplace_back(item.Level, item.SstPtr->AssignedSstId);
    }
    return result;
}

void AssertDiskPartsEqual(const TDiskPartVec& lhs, const TDiskPartVec& rhs) {
    UNIT_ASSERT_VALUES_EQUAL(lhs.Size(), rhs.Size());
    auto other = rhs.begin();
    for (const auto& part : lhs) {
        UNIT_ASSERT_VALUES_EQUAL(part, *other++);
    }
}

template<class TBase>
void AssertBaseEqual(const TBase& lhs, const TBase& rhs) {
    UNIT_ASSERT_VALUES_EQUAL(lhs.Finalized, rhs.Finalized);
    UNIT_ASSERT_VALUES_EQUAL(SstIds(lhs.TablesToDelete), SstIds(rhs.TablesToDelete));
    UNIT_ASSERT_VALUES_EQUAL(SstIds(lhs.TablesToAdd), SstIds(rhs.TablesToAdd));
    AssertDiskPartsEqual(lhs.HugeBlobsToDelete, rhs.HugeBlobsToDelete);
    AssertDiskPartsEqual(lhs.HugeBlobsAllocated, rhs.HugeBlobsAllocated);
    AssertDiskPartsEqual(lhs.HugeBlobsAllocatedStripe, rhs.HugeBlobsAllocatedStripe);
}

template<class TKey, class TMemRec>
void AssertTasksEqual(const TTask<TKey, TMemRec>& lhs, const TTask<TKey, TMemRec>& rhs) {
    UNIT_ASSERT_VALUES_EQUAL(ui32(lhs.Action), ui32(rhs.Action));
    UNIT_ASSERT_VALUES_EQUAL(ui32(lhs.SelectStrategy), ui32(rhs.SelectStrategy));
    UNIT_ASSERT_VALUES_EQUAL(lhs.IsFullCompaction, rhs.IsFullCompaction);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Priority.MaxRank, rhs.Priority.MaxRank);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Priority.EmergencyMode, rhs.Priority.EmergencyMode);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.Valid, rhs.Forecast.Valid);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.InputChunks, rhs.Forecast.InputChunks);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.OutputChunks, rhs.Forecast.OutputChunks);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.StripeBlocksAllocated, rhs.Forecast.StripeBlocksAllocated);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.StripeBlocksReleased, rhs.Forecast.StripeBlocksReleased);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Forecast.HugeGarbageBytes, rhs.Forecast.HugeGarbageBytes);
    UNIT_ASSERT_VALUES_EQUAL(lhs.FullCompactionInfo.second, rhs.FullCompactionInfo.second);
    const auto& a = lhs.FullCompactionInfo.first;
    const auto& b = rhs.FullCompactionInfo.first;
    UNIT_ASSERT_VALUES_EQUAL(a.has_value(), b.has_value());
    if (a) {
        UNIT_ASSERT_VALUES_EQUAL(a->FullCompactionLsn, b->FullCompactionLsn);
        UNIT_ASSERT_VALUES_EQUAL(a->CompactionStartTime, b->CompactionStartTime);
        // TFullCompactionAttrs::operator== does not compare TablesToCompact.
        UNIT_ASSERT(a->TablesToCompact == b->TablesToCompact);
    }
    AssertBaseEqual(lhs.DeleteSsts, rhs.DeleteSsts);
    AssertBaseEqual(lhs.MoveSsts, rhs.MoveSsts);
    AssertBaseEqual(lhs.CompactSsts, rhs.CompactSsts);
    UNIT_ASSERT_VALUES_EQUAL(lhs.CompactSsts.TargetLevel, rhs.CompactSsts.TargetLevel);
    UNIT_ASSERT_C(lhs.CompactSsts.LastCompactedKey == rhs.CompactSsts.LastCompactedKey,
        lhs.CompactSsts.LastCompactedKey.ToString() << " != " << rhs.CompactSsts.LastCompactedKey.ToString());
    const auto& chains = lhs.CompactSsts.CompactionChains;
    const auto& other = rhs.CompactSsts.CompactionChains;
    UNIT_ASSERT_VALUES_EQUAL(chains.size(), other.size());
    for (size_t i = 0; i < chains.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(chains[i]->Segments.size(), other[i]->Segments.size());
        for (size_t j = 0; j < chains[i]->Segments.size(); ++j) {
            UNIT_ASSERT_VALUES_EQUAL(chains[i]->Segments[j]->AssignedSstId,
                other[i]->Segments[j]->AssignedSstId);
        }
    }
}

template<class TKey = TKeyLogoBlob, class TMemRec = TMemRecLogoBlob, class TSetup>
TSelectionResult<TKey, TMemRec> CheckEquivalent(TSetup setup, EAction action,
        ESelectStrategy strategy,
        std::function<void(TInput<TKey, TMemRec>&)> whileSuspended = {}) {
    TEnvironment env(1, TDuration::MilliSeconds(1));
    // Separate SSTs are essential: selection updates the shared StorageRatio cache.
    TInput<TKey, TMemRec> syncInput(env.System.get());
    TInput<TKey, TMemRec> coroInput(env.System.get());
    setup(syncInput);
    setup(coroInput);
    auto sync = RunSelector<TKey, TMemRec, TSelectorActor>(env, syncInput);
    auto coro = RunSelector<TKey, TMemRec, TSelectorActorCoro>(env, coroInput, whileSuspended ? std::function<void()>([&] { whileSuspended(coroInput); }) : std::function<void()>{});
    UNIT_ASSERT_VALUES_EQUAL(TString(ActionToStr(sync.Action)), TString(ActionToStr(action)));
    UNIT_ASSERT_VALUES_EQUAL(TString(ActionToStr(coro.Action)), TString(ActionToStr(action)));
    UNIT_ASSERT(sync.Task && coro.Task);
    UNIT_ASSERT_VALUES_EQUAL(ui32(sync.Task->Action), ui32(sync.Action));
    UNIT_ASSERT_VALUES_EQUAL(ui32(coro.Task->Action), ui32(coro.Action));
    UNIT_ASSERT_VALUES_EQUAL(ui32(sync.Task->SelectStrategy), ui32(strategy));
    AssertTasksEqual(*sync.Task, *coro.Task);
    UNIT_ASSERT_VALUES_EQUAL(sync.Resumes, 0);
    UNIT_ASSERT_C(coro.Resumes > 0, "equivalence must also hold after a real coroutine suspension");
    UNIT_ASSERT_VALUES_EQUAL(sync.Ratios.size(), coro.Ratios.size());
    for (size_t i = 0; i < sync.Ratios.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(bool(sync.Ratios[i]), bool(coro.Ratios[i]));
        if (sync.Ratios[i]) {
            // ToString includes every ratio counter, but not calculation timestamps.
            UNIT_ASSERT_VALUES_EQUAL(sync.Ratios[i]->ToString(), coro.Ratios[i]->ToString());
        }
    }
    return coro;
}

template<class TSetup>
void CheckAllDatabases(TSetup setup, EAction action, ESelectStrategy strategy) {
    CheckEquivalent<TKeyLogoBlob, TMemRecLogoBlob>(setup, action, strategy);
    CheckEquivalent<TKeyBlock, TMemRecBlock>(setup, action, strategy);
    CheckEquivalent<TKeyBarrier, TMemRecBarrier>(setup, action, strategy);
}

void CheckSuspendedCleanup(bool poison) {
    // Observers must outlive actor-system teardown, including assertion failures.
    std::atomic<ui32> resumes = 0;
    std::promise<TAutoPtr<IEventHandle>> suspended;
    std::promise<void> destroyed;
    auto suspension = suspended.get_future();
    auto destruction = destroyed.get_future();
    TEnvironment env;
    // These inputs outlive pool cleanup, so their deleters must not send to Batch.
    TInput<TKeyLogoBlob, TMemRecLogoBlob> input(nullptr);
    auto sst = input.AddSst(0, 1, 500'000, 10);
    sst->StorageRatio.Set(MakeIntrusive<TSstRatio>(), TInstant::Zero());
    const auto refs = input.Index->CurSlice.RefCount();

    std::promise<TSelectionResult<TKeyLogoBlob, TMemRecLogoBlob>> promise;
    auto result = promise.get_future();
    const auto recipient = env.System->Register(
        new TResultReceiver<TKeyLogoBlob, TMemRecLogoBlob>(std::move(promise)));
    auto* selector = new TSelectorActorCoro<TKeyLogoBlob, TMemRecLogoBlob>(input.HullCtx, input.Params,
        input.Index->GetIndexSnapshot(), input.Ds->Barriers->GetIndexSnapshot(), recipient,
        std::make_unique<TTask<TKeyLogoBlob, TMemRecLogoBlob>>(), input.AllowGarbageCollection);
    const auto id = env.System->Register(new TResumeCounter(selector, resumes, &suspended, &destroyed),
        TMailboxType::HTSwap, 0);
    UNIT_ASSERT_C(suspension.wait_for(std::chrono::seconds(60)) == std::future_status::ready,
        "selector did not suspend");
    UNIT_ASSERT(suspension.get()); // Drop the resume event, leaving the coroutine suspended.
    UNIT_ASSERT(input.Index->CurSlice.RefCount() > refs);

    if (poison) {
        env.System->Send(new IEventHandle(id, {}, new TEvents::TEvPoisonPill));
    } else {
        env.System->Cleanup();
    }
    UNIT_ASSERT_C(destruction.wait_for(std::chrono::seconds(60)) == std::future_status::ready,
        "suspended selector was not destroyed");
    UNIT_ASSERT_VALUES_EQUAL(input.Index->CurSlice.RefCount(), refs);
    if (poison) {
        env.System->Send(new IEventHandle(recipient, {}, new TEvents::TEvWakeup));
        UNIT_ASSERT(result.wait_for(std::chrono::seconds(60)) == std::future_status::ready);
        UNIT_ASSERT_C(!result.get().Task, "cancelled selector returned a compaction task");
    }
}

Y_UNIT_TEST_SUITE(SelectorEquivalence) {
    Y_UNIT_TEST(RecalculateStorageRatioAfterYield) {
        constexpr ui32 records = 500'000;
        auto result = CheckEquivalent([](auto& input) {
            auto sst = input.AddSst(0, 1, records, 10);
            sst->StorageRatio.Set(MakeIntrusive<TSstRatio>(), TInstant::Zero());
        }, ActNothing, ESelectStrategy::None);
        UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsTotal, records);
        UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsKeep, records);
    }

    Y_UNIT_TEST(GarbageCollectionAfterYield) {
        constexpr ui32 records = 500'000;
        for (bool allowGc : {false, true}) {
            auto result = CheckEquivalent([&](auto& input) {
                auto sst = input.AddSst(LastLevel, 1, records, 10);
                sst->StorageRatio.Set(MakeIntrusive<TSstRatio>(), TInstant::Zero());
                input.Ds->Barriers->PutToFresh(1, TKeyBarrier(1, 0, 2, 1, true),
                    TMemRecBarrier(1, records / 2, TBarrierIngress::CreateFromRaw(Max<ui32>())));
                input.AllowGarbageCollection = allowGc;
            }, ActNothing, ESelectStrategy::None);
            UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsTotal, records);
            UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsKeep, allowGc ? records / 2 : records);
        }
    }

    Y_UNIT_TEST(DeleteHugeBlobsAfterYield) {
        constexpr ui32 records = 300'000;
        auto result = CheckEquivalent([](auto& input) {
            auto sst = input.AddSst(LastLevel, 1, records, 10, true);
            auto ratio = sst->StorageRatio.Get();
            ratio->IndexItemsKeep = ratio->IndexBytesKeep = ratio->HugeDataKeep = 0;
        }, ActDeleteSsts, ESelectStrategy::DelSst);
        UNIT_ASSERT_VALUES_EQUAL(result.Task->DeleteSsts.HugeBlobsToDelete.Size(), records);
    }
    Y_UNIT_TEST(ManySstsExplicitAfterYield) {
        CheckAllDatabases([](auto& input) {
            for (ui32 i = 1; i <= 100'000; ++i) {
                input.AddSst(LastLevel, i, i, i);
            }
            input.Params.FullCompactionAttrs.emplace(1, TInstant::Seconds(100), THashSet<ui64>{1, 100'000});
        }, ActCompactSsts, ESelectStrategy::Explicit);
    }

    Y_UNIT_TEST(ManySstsNeighborhoodAfterYield) {
        CheckAllDatabases([](auto& input) {
            input.Params.RankThreshold = 1e9;
            input.AddSst(LastLevel - 1, 1, 100'000, 100'001);
            for (ui32 i = 1; i <= 100'000; ++i) {
                input.AddSst(LastLevel, i, i, i);
            }
            input.Params.FullCompactionAttrs.emplace(1, TInstant::Seconds(100), THashSet<ui64>{});
        }, ActCompactSsts, ESelectStrategy::BalanceFull);
    }

    Y_UNIT_TEST(ManySstsDeleteAfterYield) {
        auto result = CheckEquivalent([](auto& input) {
            for (ui32 i = 1; i <= 100'000; ++i) {
                auto sst = input.AddSst(LastLevel, i, i, i);
                auto ratio = sst->StorageRatio.Get();
                ratio->IndexItemsKeep = ratio->IndexBytesKeep = ratio->InplacedDataKeep = 0;
            }
        }, ActDeleteSsts, ESelectStrategy::DelSst);
        UNIT_ASSERT_VALUES_EQUAL(SstIds(result.Task->DeleteSsts.TablesToDelete).size(), 100'000);
    }

    Y_UNIT_TEST(ManySstsPromoteAfterYield) {
        CheckAllDatabases([](auto& input) {
            for (ui32 i = 1; i <= 100'001; ++i) {
                input.AddSst(LastLevel - 1, i, i, i);
            }
            input.AddSst(LastLevel, 1, 100'000, 100'002);
        }, ActMoveSsts, ESelectStrategy::PromoteSsts);
    }

    Y_UNIT_TEST(ManySstsEmergencyAfterYield) {
        CheckEquivalent([](auto& input) {
            for (ui32 i = 1; i <= 50'000; ++i) {
                input.AddSst(LastLevel, i, i, i);
            }
            input.Params.EmergencyMode = true;
            input.Params.FreeChunksBudget = 1;
        }, ActCompactSsts, ESelectStrategy::Emergency);
    }

    Y_UNIT_TEST(ManySstsFreeSpaceAfterYield) {
        CheckEquivalent([](auto& input) {
            input.Ctx.GetHullCtx()->VCfg->HullCompEmergencyMaxSsts = 0;
            input.Ctx.GetHullCtx()->VCfg->HullCompFreeSpaceThresholdPerMille = 500;
            input.Params.EmergencyMode = true;
            for (ui32 i = 1; i <= 100'000; ++i) {
                input.AddSst(LastLevel, i, i, i);
            }
            auto ratio = input.Ssts.back()->StorageRatio.Get();
            ratio->HugeDataTotal = ChunkSize;
            ratio->HugeDataKeep = 0;
        }, ActCompactSsts, ESelectStrategy::FreeSpace);
    }

    Y_UNIT_TEST(ManySstsSqueezeAfterYield) {
        CheckEquivalent([](auto& input) {
            input.Ctx.GetHullCtx()->VCfg->HullCompEmergencyMaxSsts = 0;
            input.Params.EmergencyMode = true;
            for (ui32 i = 1; i <= 100'000; ++i) {
                input.AddSst(LastLevel, i, i, i)->Info.CTime = TInstant::Seconds(10);
            }
            input.Ssts.back()->Info.CTime = TInstant::Seconds(1);
            input.Params.SqueezeBefore = TInstant::Seconds(2);
        }, ActCompactSsts, ESelectStrategy::Squeeze);
    }

    Y_UNIT_TEST(StorageRatioSuspensionDoesNotConsumeBudget) {
        const auto setup = [](auto& input) {
            input.HullCtx->VCfg->HullCompEmergencyMaxSsts = 0;
            for (ui32 i = 0; i < 3; ++i) {
                auto sst = input.AddSst(LastLevel, i * 300'000 + 1, (i + 1) * 300'000, i + 1);
                sst->StorageRatio.Get()->Time = TInstant::Zero();
                sst->StorageRatio.SetCalculationTime(TInstant::Zero());
            }
        };
        auto result = CheckEquivalent<TKeyLogoBlob, TMemRecLogoBlob>(setup, ActNothing, ESelectStrategy::None,
            [](auto& input) {
                // The resume event is held outside the mailbox; no executor worker is blocked.
                std::this_thread::sleep_for(std::chrono::microseconds(
                    (input.HullCtx->HullCompStorageRatioMaxCalcDuration + TDuration::MilliSeconds(200)).MicroSeconds()));
            });
        for (const auto& ratio : result.Ratios) {
            UNIT_ASSERT(ratio->Time != TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(ratio->IndexItemsTotal, 300'000);
        }
    }

    Y_UNIT_TEST(StorageRatioComputationConsumesBudget) {
        TEnvironment env;
        TInput<TKeyLogoBlob, TMemRecLogoBlob> input(env.System.get());
        const auto& c = *input.HullCtx;
        input.HullCtx = MakeIntrusive<THullCtx>(c.VCtx, c.VCfg, c.ChunkSize, c.CompWorthReadSize,
            c.FreshCompaction, c.GCOnlySynced, c.AllowKeepFlags, c.BarrierValidation,
            c.HullSstSizeInChunksFresh, c.HullSstSizeInChunksLevel, c.HullCompReadBatchEfficiencyThreshold,
            c.HullCompStorageRatioCalcPeriod, TDuration::MicroSeconds(1), c.HullCompLevel0MaxSstsAtOnce,
            c.HullCompSortedPartsNum);
        for (ui32 i = 0; i < 3; ++i) {
            auto sst = input.AddSst(LastLevel, i * 300'000 + 1, (i + 1) * 300'000, i + 1);
            sst->StorageRatio.Get()->Time = TInstant::Zero();
            sst->StorageRatio.SetCalculationTime(TInstant::Zero());
        }
        auto result = RunSelector<TKeyLogoBlob, TMemRecLogoBlob, TSelectorActorCoro>(env, input);
        UNIT_ASSERT(result.Resumes > 0);
        ui32 recalculated = 0;
        for (const auto& ratio : result.Ratios) {
            recalculated += ratio->Time != TInstant::Zero();
        }
        UNIT_ASSERT_VALUES_EQUAL(recalculated, 1);
    }

    Y_UNIT_TEST(SnapshotChangesWhileSuspended) {
        TIntrusivePtr<TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>> oldSlice;
        auto result = CheckEquivalent<TKeyLogoBlob, TMemRecLogoBlob>([](auto& input) {
            auto sst = input.AddSst(LastLevel, 1, 500'000, 10);
            sst->StorageRatio.Get()->Time = TInstant::Zero();
            sst->StorageRatio.SetCalculationTime(TInstant::Zero());
            // Include Fresh in the original snapshot too.
            TMemRecLogoBlob memRec;
            input.Index->PutToFresh(2, input.MakeKey(500'000), memRec);
        }, ActNothing, ESelectStrategy::None, [&](auto& input) {
            oldSlice = input.Index->CurSlice;
            UNIT_ASSERT(oldSlice.RefCount() > 2);
            TMemRecLogoBlob memRec;
            input.Index->PutToFresh(3, input.MakeKey(500'001), memRec);
            auto fresh = input.Index->FindFreshSegmentForCompaction();
            input.Index->FreshCompactionSstCreated(std::move(fresh));
            input.Index->FreshCompactionFinished();
            input.Ds->Barriers->PutToFresh(4, TKeyBarrier(1, 0, 2, 1, true),
                TMemRecBarrier(1, 500'000, TBarrierIngress::CreateFromRaw(Max<ui32>())));
            input.Index->CurSlice = MakeIntrusive<TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>>(
                input.Ctx.GetLevelIndexSettings(), oldSlice->Ctx);
            for (ui32 i = 0; i < LastLevel; ++i) {
                input.Index->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob::First());
            }
            input.AddSst(LastLevel, 500'001, 500'010, 11);
            input.Ssts.pop_back(); // Only compare the SSTs visible to the selector's snapshot.
        });
        UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsKeep, 500'000);
        UNIT_ASSERT_VALUES_EQUAL(oldSlice.RefCount(), 1);
    }

    Y_UNIT_TEST(OverlappingKeepFlagsAfterYield) {
        auto result = CheckEquivalent([](auto& input) {
            const auto flags = [](ui32 step) {
                TIngress ingress;
                ingress.SetKeep(TIngress::EMode::GENERIC,
                    step % 2 ? CollectModeKeep : CollectModeDoNotKeep);
                return ingress;
            };
            input.AddSst(LastLevel - 1, 1, 500'000, 10, false, flags);
            input.AddSst(LastLevel, 1, 500'000, 11);
            for (const auto& sst : input.Ssts) {
                sst->StorageRatio.Get()->Time = TInstant::Zero();
                sst->StorageRatio.SetCalculationTime(TInstant::Zero());
            }
            input.Ds->Barriers->PutToFresh(1, TKeyBarrier(1, 0, 2, 1, false),
                TMemRecBarrier(1, 500'000, TBarrierIngress::CreateFromRaw(Max<ui32>())));
            input.Ctx.GetHullCtx()->VCfg->HullCompEmergencyMaxSsts = 0;
            input.Params.EmergencyMode = true;
        }, ActNothing, ESelectStrategy::None);
        UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(0)->IndexItemsKeep, 250'000);
        UNIT_ASSERT_VALUES_EQUAL(result.Ratios.at(1)->IndexItemsKeep, 250'000);
    }

}

Y_UNIT_TEST_SUITE(SelectorLifetime) {
    Y_UNIT_TEST(PoisonWhileSuspended) { CheckSuspendedCleanup(true); }
    Y_UNIT_TEST(ShutdownWhileSuspended) { CheckSuspendedCleanup(false); }
}

} // namespace
} // namespace NKikimr
