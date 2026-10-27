#include <yql/essentials/minikql/async/scheduler.h>
#include <library/cpp/testing/unittest/registar.h>
#include <atomic>
#include <thread>

namespace NKikimr::NMiniKQL::NAsync {
namespace {
struct TBatch final: IBatchProvider {
    std::vector<TBatchItem> Items;
    std::vector<std::vector<TOperationId>> Batches;
    std::vector<TOperationId> Cancelled;
    bool Throw = false;
    bool LargeKey = false;
    TString GetBatchKey(const TRequest& r) const override {
        return LargeKey ? TString(1025, 'K') : r.Payload.substr(0, 1);
    }
    size_t GetMaxBatchSize() const noexcept override {
        return 64;
    }
    void StartBatch(std::vector<TBatchItem> items) override {
        std::vector<TOperationId> ids;
        for (auto& item : items) {
            ids.push_back(item.Id);
            Items.push_back(std::move(item));
        }
        Batches.push_back(std::move(ids));
        if (Throw) {
            ythrow yexception() << "start failed";
        }
    }
    void Cancel(TOperationId id) noexcept override {
        Cancelled.push_back(id);
    }
    void Release(size_t index) {
        Items.at(index).Request.reset();
        Items.at(index).Completion->Release();
    }
    void Reply(size_t index, bool release = true) {
        auto& item = Items.at(index);
        item.Completion->Complete(item.Request->Payload);
        if (release) {
            Release(index);
        }
    }
};
struct TScalar final: IProvider {
    std::vector<TBatchItem> Items;
    void Start(TOperationId id, std::shared_ptr<const TRequest> request, std::shared_ptr<ICompletion> completion) override {
        Items.push_back({id, std::move(request), std::move(completion)});
    }
    void Cancel(TOperationId) noexcept override {
    }
};
TRequest Request(TString payload = "A", TString operation = "lookup", std::optional<TTimePoint> deadline = {}) {
    return {std::move(operation), std::move(payload), 128, deadline};
}
void Flush(TScheduler& scheduler) {
    UNIT_ASSERT(!scheduler.Flush([] { return true; }));
}
} // namespace
Y_UNIT_TEST_SUITE(TAsyncScheduler) {
Y_UNIT_TEST(RealBatchesAndPartialTailAtWorkBoundary) {
    for (size_t batchSize : {1u, 8u, 64u}) {
        auto provider = std::make_shared<TBatch>();
        TSchedulerSettings settings;
        settings.MaxBatchSize = batchSize;
        settings.MaxInFlight = 128;
        TScheduler scheduler([](ui64) {}, {}, settings);
        scheduler.RegisterProvider("native", provider);
        for (size_t i = 0; i < batchSize + 3; ++i) {
            UNIT_ASSERT(scheduler.Submit("native", Request()).Status == EAdmission::Accepted);
        }
        UNIT_ASSERT(provider->Items.empty());
        Flush(scheduler);
        UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), batchSize + 3);
        UNIT_ASSERT_VALUES_EQUAL(provider->Batches.front().size(), batchSize);
        UNIT_ASSERT_VALUES_EQUAL(provider->Batches.back().size(), batchSize == 1 ? 1 : 3);
        for (size_t i = 0; i < provider->Items.size(); ++i) {
            provider->Reply(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(scheduler.TakeReady().size(), batchSize + 3);
        UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
    }
}
Y_UNIT_TEST(SeparateSecurityKeysContractsDeadlinesAndRegistrations) {
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("tenant1", provider);
    scheduler.RegisterProvider("tenant2", provider);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::hours(1);
    scheduler.Submit("tenant1", Request("A1"));
    scheduler.Submit("tenant1", Request("B1"));
    scheduler.Submit("tenant1", Request("A2"));
    scheduler.Submit("tenant1", Request("A3", "other"));
    scheduler.Submit("tenant1", Request("A4", "lookup", deadline));
    scheduler.Submit("tenant2", Request("A5"));
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(provider->Batches.size(), 5);
    UNIT_ASSERT_VALUES_EQUAL(provider->Batches.front().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(provider->Items[0].Request->Payload, "A1");
    UNIT_ASSERT_VALUES_EQUAL(provider->Items[1].Request->Payload, "A2");
    UNIT_ASSERT_VALUES_EQUAL(provider->Items[2].Request->Payload, "A5");
}
Y_UNIT_TEST(ScalarFallbackUsesTheSameAdmissionAndResults) {
    auto provider = std::make_shared<TScalar>();
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("scalar", provider);
    auto first = scheduler.Submit("scalar", Request("one"));
    auto second = scheduler.Submit("scalar", Request("two"));
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), 2);
    provider->Items[1].Completion->Complete("two");
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
    UNIT_ASSERT(ready[0].Id == second.Id);
    UNIT_ASSERT(ready[0].Id != first.Id);
}
Y_UNIT_TEST(PartialOutOfOrderCompletionAndCancellationArePerItem) {
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("native", provider);
    std::vector<TOperationId> ids;
    for (size_t i = 0; i < 8; ++i) {
        ids.push_back(scheduler.Submit("native", Request()).Id);
    }
    Flush(scheduler);
    provider->Reply(7);
    provider->Reply(3);
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 2);
    UNIT_ASSERT(ready[0].Id == ids[7] && ready[1].Id == ids[3]);
    UNIT_ASSERT(scheduler.Cancel(ids[2]));
    ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
    UNIT_ASSERT(ready[0].Id == ids[2] && ready[0].Outcome == EOutcome::Cancelled);
    UNIT_ASSERT_VALUES_EQUAL(provider->Cancelled.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetSchedulerStats().InFlight, 6);
    provider->Release(2);
    for (size_t i : {0u, 1u, 4u, 5u, 6u}) {
        provider->Reply(i);
    }
    ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 5);
    for (auto& result : ready) {
        UNIT_ASSERT(result.Outcome == EOutcome::Success);
    }
}
Y_UNIT_TEST(InflightCreditWaitsForPhysicalCleanup) {
    std::atomic<size_t> events = 0;
    auto provider = std::make_shared<TBatch>();
    TSchedulerSettings settings;
    settings.MaxInFlight = 1;
    TScheduler scheduler([&](ui64) { ++events; }, {}, settings);
    scheduler.RegisterProvider("native", provider);
    auto first = scheduler.Submit("native", Request());
    scheduler.Submit("native", Request());
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), 1);
    scheduler.Cancel(first.Id);
    scheduler.TakeReady();
    events = 0;
    size_t steps = 0;
    UNIT_ASSERT(!scheduler.Flush([&] { ++steps; return true; }));
    UNIT_ASSERT_VALUES_EQUAL(steps, 0);
    UNIT_ASSERT_VALUES_EQUAL(events.load(), 0);
    std::thread cleanup([&] { provider->Release(0); });
    cleanup.join();
    UNIT_ASSERT(events.load());
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), 2);
}
Y_UNIT_TEST(ReadyResultsRetainQuotaAndWakeBlockedAdmission) {
    std::atomic<size_t> events = 0;
    auto provider = std::make_shared<TBatch>();
    TSchedulerSettings settings;
    settings.MaxMemoryBytes = 4000;
    TScheduler scheduler([&](ui64) { ++events; }, {}, settings);
    scheduler.RegisterProvider("native", provider);
    UNIT_ASSERT(scheduler.Submit("native", Request()).Status == EAdmission::Accepted);
    Flush(scheduler);
    provider->Reply(0);
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetStats().TrackedOperations, 0);
    UNIT_ASSERT(scheduler.GetQuota()->GetUsed() > 0);
    events = 0;
    UNIT_ASSERT(scheduler.Submit("native", Request()).Status == EAdmission::ResourceLimit);
    UNIT_ASSERT_VALUES_EQUAL(events.load(), 0);
    std::thread consumer([results = std::move(ready)]() mutable { results.clear(); });
    consumer.join();
    UNIT_ASSERT(events.load());
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
    UNIT_ASSERT(scheduler.Submit("native", Request()).Status == EAdmission::Accepted);
}
Y_UNIT_TEST(SharedQuotaWakesAnotherExecutionWithoutPolling) {
    auto quota = std::make_shared<TMemoryQuota>(4000);
    TSchedulerSettings settings;
    settings.Quota = quota;
    size_t secondEvents = 0;
    TScheduler first([](ui64) {}, {}, settings);
    TScheduler second([&](ui64) { ++secondEvents; }, {}, settings);
    auto p1 = std::make_shared<TBatch>();
    auto p2 = std::make_shared<TBatch>();
    first.RegisterProvider("p", p1);
    second.RegisterProvider("p", p2);
    first.Submit("p", Request());
    Flush(first);
    UNIT_ASSERT(second.Submit("p", Request()).Status == EAdmission::ResourceLimit);
    UNIT_ASSERT_VALUES_EQUAL(secondEvents, 0);
    p1->Reply(0);
    first.TakeReady();
    UNIT_ASSERT(secondEvents);
    UNIT_ASSERT(second.Submit("p", Request()).Status == EAdmission::Accepted);
}
Y_UNIT_TEST(ImpossibleRequestAndMemoryLimitFailWithoutLeakingQuota) {
    TSchedulerSettings settings;
    settings.MaxMemoryBytes = 4000;
    TScheduler scheduler([](ui64) {}, {.MaxOperations = 1}, settings);
    scheduler.RegisterProvider("p", std::make_shared<TBatch>());
    auto request = Request();
    request.MaxResponseBytes = 10000;
    UNIT_ASSERT(scheduler.Submit("p", request).Status == EAdmission::RequestTooLarge);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
    auto id = scheduler.Submit("p", Request()).Id;
    const auto before = scheduler.GetQuota()->GetUsed();
    UNIT_ASSERT(scheduler.Submit("p", Request()).Status == EAdmission::ResourceLimit);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), before);
    scheduler.Cancel(id);
    scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
}
Y_UNIT_TEST(OperationLimitBoundsQueuedAndReadyEntriesWithoutLeakingQuota) {
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {}, {.MaxOperations = 2});
    scheduler.RegisterProvider("p", provider);
    auto first = scheduler.Submit("p", Request());
    auto second = scheduler.Submit("p", Request());
    UNIT_ASSERT(first.Status == EAdmission::Accepted);
    UNIT_ASSERT(second.Status == EAdmission::Accepted);
    const auto bytes = scheduler.GetQuota()->GetUsed();
    UNIT_ASSERT(scheduler.Submit("p", Request()).Status == EAdmission::OperationsLimit);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), bytes);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetSchedulerStats().Queued, 2);
    UNIT_ASSERT(provider->Items.empty());
    scheduler.Cancel(first.Id);
    UNIT_ASSERT(scheduler.Submit("p", Request()).Status == EAdmission::OperationsLimit);
    scheduler.TakeReady();
    UNIT_ASSERT(scheduler.Submit("p", Request()).Status == EAdmission::Accepted);
    scheduler.Shutdown();
    scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
    UNIT_ASSERT(provider->Items.empty());
}
Y_UNIT_TEST(CancelQueuedItemNeverStartsBackend) {
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("p", provider);
    auto id = scheduler.Submit("p", Request()).Id;
    scheduler.Cancel(id);
    Flush(scheduler);
    UNIT_ASSERT(provider->Items.empty());
    UNIT_ASSERT(provider->Cancelled.empty());
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
    UNIT_ASSERT(ready[0].Outcome == EOutcome::Cancelled);
}
Y_UNIT_TEST(OversizedCompatibilityKeyDoesNotCancelAnUnstartedBackend) {
    auto provider = std::make_shared<TBatch>();
    provider->LargeKey = true;
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("p", provider);
    auto submitted = scheduler.Submit("p", Request());
    UNIT_ASSERT(submitted.Status == EAdmission::Accepted);
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
    UNIT_ASSERT(ready[0].Error == EError::StartFailed);
    UNIT_ASSERT(provider->Items.empty());
    UNIT_ASSERT(provider->Cancelled.empty());
}
Y_UNIT_TEST(ExpiredQueuedItemNeverStartsBackend) {
    TTimePoint now{};
    TSchedulerSettings settings;
    settings.Clock = [&] { return now; };
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {}, {}, settings);
    scheduler.RegisterProvider("p", provider);
    const auto deadline = now + std::chrono::milliseconds(1);
    scheduler.Submit("p", Request("A", "lookup", deadline));
    UNIT_ASSERT(scheduler.NextDeadline() == deadline);
    now = deadline;
    Flush(scheduler);
    UNIT_ASSERT(provider->Items.empty());
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
    UNIT_ASSERT(ready[0].Outcome == EOutcome::Timeout);
}
Y_UNIT_TEST(RoundRobinLetsColdProviderRunBeforeHotBacklog) {
    TSchedulerSettings settings;
    settings.MaxBatchSize = 1;
    settings.MaxInFlight = 1;
    auto hot = std::make_shared<TBatch>();
    auto cold = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {}, {}, settings);
    scheduler.RegisterProvider("hot", hot);
    scheduler.RegisterProvider("cold", cold);
    for (size_t i = 0; i < 64; ++i) {
        scheduler.Submit("hot", Request());
    }
    scheduler.Submit("cold", Request());
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(hot->Items.size(), 1);
    hot->Reply(0);
    scheduler.TakeReady();
    Flush(scheduler);
    UNIT_ASSERT_VALUES_EQUAL(cold->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(hot->Items.size(), 1);
}
Y_UNIT_TEST(DispatchBudgetReturnsRunnableButCapacityWaitDoesNot) {
    auto provider = std::make_shared<TBatch>();
    TSchedulerSettings settings;
    settings.MaxInFlight = 1;
    TScheduler scheduler([](ui64) {}, {}, settings);
    scheduler.RegisterProvider("p", provider);
    scheduler.Submit("p", Request());
    scheduler.Submit("p", Request());
    UNIT_ASSERT(scheduler.Flush([] { return false; }));
    UNIT_ASSERT(provider->Items.empty());
    Flush(scheduler);
    UNIT_ASSERT(!scheduler.Flush([] { return false; }));
    UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), 1);
}
Y_UNIT_TEST(ShutdownRetainsCleanupAndSuppressesLateWakeups) {
    std::atomic<size_t> events = 0;
    auto provider = std::make_shared<TBatch>();
    TSchedulerSettings settings;
    settings.MaxInFlight = 1;
    TScheduler scheduler([&](ui64) { ++events; }, {}, settings);
    scheduler.RegisterProvider("p", provider);
    scheduler.Submit("p", Request());
    scheduler.Submit("p", Request());
    Flush(scheduler);
    scheduler.Shutdown();
    scheduler.TakeReady();
    events = 0;
    UNIT_ASSERT_VALUES_EQUAL(provider->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(provider->Cancelled.size(), 1);
    UNIT_ASSERT(scheduler.GetQuota()->GetUsed());
    std::thread cleanup([&] { provider->Reply(0); });
    cleanup.join();
    UNIT_ASSERT_VALUES_EQUAL(events.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
    UNIT_ASSERT(!scheduler.Flush([] { return true; }));
}
Y_UNIT_TEST(ThrowingBatchFailsEachItemButRetainsCleanup) {
    auto provider = std::make_shared<TBatch>();
    provider->Throw = true;
    TScheduler scheduler([](ui64) {});
    scheduler.RegisterProvider("p", provider);
    scheduler.Submit("p", Request());
    scheduler.Submit("p", Request());
    Flush(scheduler);
    auto ready = scheduler.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(ready.size(), 2);
    for (auto& result : ready) {
        UNIT_ASSERT(result.Outcome == EOutcome::Error);
    }
    UNIT_ASSERT_VALUES_EQUAL(provider->Cancelled.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetSchedulerStats().InFlight, 2);
    ready.clear();
    provider->Release(0);
    provider->Release(1);
    UNIT_ASSERT_VALUES_EQUAL(scheduler.GetQuota()->GetUsed(), 0);
}
Y_UNIT_TEST(MetricsAndTraceHavePerOperationCorrelation) {
    TTimePoint now{};
    std::vector<TExecutionEvent> events;
    TSchedulerSettings settings;
    settings.Clock = [&] { return now; };
    settings.Trace = [&](auto event) { events.push_back(event); };
    auto provider = std::make_shared<TBatch>();
    TScheduler scheduler([](ui64) {}, {}, settings);
    scheduler.RegisterProvider("p", provider);
    auto id = scheduler.Submit("p", Request()).Id;
    now += std::chrono::microseconds(10);
    Flush(scheduler);
    now += std::chrono::microseconds(20);
    provider->Reply(0);
    scheduler.TakeReady();
    auto stats = scheduler.GetSchedulerStats();
    UNIT_ASSERT_VALUES_EQUAL(stats.Submitted, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.Completed, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.Dispatches, 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.QueueWaitUs, 10);
    UNIT_ASSERT_VALUES_EQUAL(stats.ExecutionWaitUs, 20);
    UNIT_ASSERT_VALUES_EQUAL(events.size(), 3);
    for (auto& event : events) {
        UNIT_ASSERT(event.Id == id);
    }
}
Y_UNIT_TEST(QuotaWaiterReplacementAndRemovalDestroyCapturesOutsideLock) {
    auto quota = std::make_shared<TMemoryQuota>(1);
    auto held = quota->TryReserve(1);
    struct TReenter {
        std::shared_ptr<TMemoryQuota> Quota;
        ~TReenter() {
            Quota->GetUsed();
        }
    };
    auto capture = std::make_shared<TReenter>();
    capture->Quota = quota;
    UNIT_ASSERT(!quota->TryReserve(1, 7, [capture] {}));
    capture.reset();
    UNIT_ASSERT(!quota->TryReserve(1, 7, [] {}));
    capture = std::make_shared<TReenter>();
    capture->Quota = quota;
    UNIT_ASSERT(!quota->TryReserve(1, 7, [capture] {}));
    capture.reset();
    quota->RemoveWaiter(7);
    held.reset();
    UNIT_ASSERT_VALUES_EQUAL(quota->GetUsed(), 0);
}
} // Y_UNIT_TEST_SUITE(TAsyncScheduler)
} // namespace NKikimr::NMiniKQL::NAsync
