#include <yql/essentials/minikql/async/async.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <atomic>
#include <barrier>
#include <limits>
#include <thread>

using namespace NKikimr::NMiniKQL::NAsync;

namespace {

struct TCall {
    TOperationId Id;
    std::weak_ptr<const TRequest> Request;
    std::shared_ptr<ICompletion> Completion;
    size_t Cancellations = 0;
};

class TManualProvider final: public IProvider {
public:
    std::vector<TCall> Calls;
    std::function<void(TCall&)> OnStart;
    std::function<void(TCall&)> OnCancel;
    bool ThrowBeforeStart = false;

    void Start(TOperationId id, std::shared_ptr<const TRequest> request,
               std::shared_ptr<ICompletion> completion) override {
        if (ThrowBeforeStart) {
            ythrow yexception() << "start failed";
        }
        Calls.push_back({id, request, std::move(completion)});
        if (OnStart) {
            OnStart(Calls.back());
        }
    }

    void Cancel(TOperationId id) noexcept override {
        for (auto& call : Calls) {
            if (call.Id == id) {
                ++call.Cancellations;
                if (OnCancel) {
                    OnCancel(call);
                }
                return;
            }
        }
    }
};

TTimePoint At(ui64 tick) {
    return TTimePoint(std::chrono::microseconds(tick));
}

struct TFixture {
    std::shared_ptr<std::atomic<ui64>> Tick = std::make_shared<std::atomic<ui64>>(0);
    std::shared_ptr<std::atomic<ui64>> Wakeups = std::make_shared<std::atomic<ui64>>(0);
    std::shared_ptr<TManualProvider> Provider = std::make_shared<TManualProvider>();
    TExecutionContext Context;

    explicit TFixture(TLimits limits = {})
        : Context([count = Wakeups](ui64) { ++*count; }, limits,
                  [tick = Tick] { return At(tick->load()); })
    {
        Context.RegisterProvider("test", Provider);
    }

    TSubmission Submit(std::optional<TTimePoint> deadline = {}) {
        return Context.Submit("test", {"echo", "input", 16, deadline});
    }

    TResult TakeOne() {
        auto results = Context.TakeReady();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        return std::move(results.front());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAsyncOperationLifecycle) {
Y_UNIT_TEST(InlineCompletionIsRegisteredBeforeStart) {
    TFixture f;
    f.Provider->OnStart = [](TCall& call) {
        call.Completion->Complete("inline");
        call.Completion->Release();
    };
    const auto submission = f.Submit();
    UNIT_ASSERT(submission.Status == EAdmission::Accepted);
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 1);
    const auto result = f.TakeOne();
    UNIT_ASSERT(result.Id == submission.Id);
    UNIT_ASSERT(result.Outcome == EOutcome::Success);
    UNIT_ASSERT_VALUES_EQUAL(result.Payload, "inline");
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
}

Y_UNIT_TEST(DeferredResultAndCoalescedWakeup) {
    TFixture f;
    for (size_t i = 0; i < 3; ++i) {
        UNIT_ASSERT(f.Submit().Status == EAdmission::Accepted);
    }
    UNIT_ASSERT(f.Context.TakeReady().empty());
    std::thread producer([&] {
        for (auto& call : f.Provider->Calls) {
            call.Completion->Complete("ready");
            call.Completion->Release();
        }
    });
    producer.join();
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.TakeReady().size(), 3);
    f.Submit();
    f.Provider->Calls.back().Completion->Fail("error");
    const auto result = f.TakeOne();
    UNIT_ASSERT(result.Outcome == EOutcome::Error);
    UNIT_ASSERT(result.Error == EError::Provider);
    UNIT_ASSERT_VALUES_EQUAL(result.Payload, "error");
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), 2);
}

Y_UNIT_TEST(DuplicateCompletionAndReleaseAreHarmless) {
    TFixture f;
    f.Submit();
    const auto token = f.Provider->Calls.front().Completion;
    token->Complete("first");
    token->Fail("second");
    token->Release();
    token->Release();
    UNIT_ASSERT_VALUES_EQUAL(f.TakeOne().Payload, "first");
    token->Complete("late");
    token->Release();
    UNIT_ASSERT(f.Context.TakeReady().empty());
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
}

Y_UNIT_TEST(AbandonedCompletionBecomesAnError) {
    TFixture f;
    f.Provider->OnStart = [](TCall& call) { call.Completion.reset(); };
    f.Submit();
    const auto result = f.TakeOne();
    UNIT_ASSERT(result.Outcome == EOutcome::Error);
    UNIT_ASSERT(result.Error == EError::Abandoned);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
}

Y_UNIT_TEST(ReleaseWithoutResultCannotLeavePendingWork) {
    TFixture f;
    f.Submit();
    f.Provider->Calls.front().Completion->Release();
    const auto result = f.TakeOne();
    UNIT_ASSERT(result.Error == EError::Abandoned);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().PendingOperations, 0);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
}

Y_UNIT_TEST(StartExceptionBeforeAndAfterLaunchingWork) {
    TFixture f;
    f.Provider->ThrowBeforeStart = true;
    f.Submit();
    UNIT_ASSERT(f.TakeOne().Error == EError::StartFailed);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
    f.Provider->ThrowBeforeStart = false;
    f.Provider->OnStart = [](TCall&) { ythrow yexception() << "failed after launch"; };
    f.Submit();
    UNIT_ASSERT(f.TakeOne().Error == EError::StartFailed);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.front().Cancellations, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().CleanupPending, 1);
    f.Provider->Calls.front().Completion->Release();
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
}

Y_UNIT_TEST(CancelDoesNotReleaseAdmissionBeforeCleanup) {
    TFixture f({1, 25}); // operation(4) + input(5) + maximum response(16)
    const auto id = f.Submit().Id;
    UNIT_ASSERT(f.Context.Cancel(id));
    UNIT_ASSERT(!f.Context.Cancel(id));
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Cancelled);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.front().Cancellations, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 25);
    UNIT_ASSERT(f.Submit().Status == EAdmission::OperationsLimit);
    f.Provider->Calls.front().Completion->Complete("ignored");
    UNIT_ASSERT(f.Context.TakeReady().empty());
    f.Provider->Calls.front().Completion->Release();
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
    UNIT_ASSERT(f.Submit().Status == EAdmission::Accepted);
}

Y_UNIT_TEST(LateCleanupWakesAdmissionWithoutAnotherCompletion) {
    TFixture f({1, 25});
    const auto first = f.Submit().Id;
    f.Context.Cancel(first);
    f.TakeOne();
    UNIT_ASSERT(f.Submit().Status == EAdmission::OperationsLimit);
    const auto before = f.Wakeups->load();
    std::thread cleanup([&] { f.Provider->Calls.front().Completion->Release(); });
    cleanup.join();
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), before + 1);
    UNIT_ASSERT(f.Context.TakeReady().empty());
    UNIT_ASSERT(f.Submit().Status == EAdmission::Accepted);
}

Y_UNIT_TEST(ResultAndCleanupBothRequiredToReturnCredits) {
    TFixture f({1, 25});
    f.Submit();
    const auto first = f.Provider->Calls.front().Completion;
    first->Complete("one");
    f.TakeOne();
    UNIT_ASSERT(f.Submit().Status == EAdmission::OperationsLimit);
    first->Release();
    f.Submit();
    const auto second = f.Provider->Calls.back().Completion;
    second->Complete("two");
    second->Release();
    UNIT_ASSERT(f.Submit().Status == EAdmission::OperationsLimit);
    f.TakeOne();
    UNIT_ASSERT(f.Submit().Status == EAdmission::Accepted);
}

Y_UNIT_TEST(CancelCanPublishAndAcknowledgeInline) {
    TFixture f;
    f.Provider->OnCancel = [](TCall& call) {
        call.Completion->Complete("too late");
        call.Completion->Release();
    };
    const auto id = f.Submit().Id;
    UNIT_ASSERT(f.Context.Cancel(id));
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Cancelled);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
}

Y_UNIT_TEST(IndependentCallCompletesWhileCancelledCallAwaitsCleanup) {
    TFixture f({2, 50});
    const auto first = f.Submit().Id;
    const auto second = f.Submit().Id;
    f.Context.Cancel(first);
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Cancelled);
    f.Provider->Calls[1].Completion->Complete("independent");
    f.Provider->Calls[1].Completion->Release();
    const auto result = f.TakeOne();
    UNIT_ASSERT(result.Id == second);
    UNIT_ASSERT(result.Outcome == EOutcome::Success);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().CleanupPending, 1);
}

Y_UNIT_TEST(ShutdownCancelsPendingButPreservesCompletedResult) {
    TFixture f;
    f.Submit();
    f.Submit();
    f.Submit();
    f.Provider->Calls[0].Completion->Complete("done");
    const auto before = f.Wakeups->load();
    f.Context.Shutdown();
    f.Context.Shutdown();
    UNIT_ASSERT(f.Context.GetStats().Closed);
    UNIT_ASSERT(f.Submit().Status == EAdmission::Closed);
    UNIT_ASSERT(!f.Context.NextDeadline());
    const auto results = f.Context.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
    UNIT_ASSERT(results[0].Outcome == EOutcome::Success);
    UNIT_ASSERT(results[1].Outcome == EOutcome::Cancelled);
    UNIT_ASSERT(results[2].Outcome == EOutcome::Cancelled);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls[0].Cancellations, 0);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls[1].Cancellations, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls[2].Cancellations, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().CleanupPending, 3);
    std::thread late([&] {
        for (auto& call : f.Provider->Calls) {
            call.Completion->Complete("late");
            call.Completion->Release();
        }
    });
    late.join();
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), before);
    UNIT_ASSERT(f.Context.TakeReady().empty());
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
}

Y_UNIT_TEST(CompletionAndOwnedInputOutliveContextSafely) {
    auto wakeups = std::make_shared<std::atomic<ui64>>(0);
    auto provider = std::make_shared<TManualProvider>();
    std::shared_ptr<ICompletion> late;
    std::weak_ptr<const TRequest> input;
    {
        TExecutionContext context([wakeups](ui64) { ++*wakeups; });
        context.RegisterProvider("test", provider);
        context.Submit("test", {"echo", "owned", 16, {}});
        late = provider->Calls.front().Completion;
        input = provider->Calls.front().Request;
    }
    UNIT_ASSERT_VALUES_EQUAL(provider->Calls.front().Cancellations, 1);
    UNIT_ASSERT(!input.expired());
    std::weak_ptr<TManualProvider> weakProvider = provider;
    provider.reset();
    UNIT_ASSERT(weakProvider.expired()); // no provider/token/registry cycle
    std::thread callback([late] {
        late->Complete("late");
        late->Release();
    });
    callback.join();
    UNIT_ASSERT(input.expired());
    UNIT_ASSERT_VALUES_EQUAL(wakeups->load(), 0);
}

Y_UNIT_TEST(InFlightWakeupSurvivesShutdownAndContextDestruction) {
    // Hold a callback after publication has released the registry mutex.
    // Destroying the context must neither wait for it nor invalidate it.
    std::barrier entered(2);
    std::barrier resume(2);
    auto delivered = std::make_shared<std::atomic<ui64>>(0);
    auto provider = std::make_shared<TManualProvider>();
    auto context = std::make_unique<TExecutionContext>([&, delivered](ui64 generation) {
        entered.arrive_and_wait();
        resume.arrive_and_wait();
        delivered->store(generation);
    });
    context->RegisterProvider("test", provider);
    const auto id = context->Submit("test", {"echo", "input", 16, {}}).Id;
    const auto token = provider->Calls.front().Completion;
    std::thread producer([token] {
        token->Complete("ready");
        token->Release();
    });
    entered.arrive_and_wait();
    context.reset();
    resume.arrive_and_wait();
    producer.join();
    UNIT_ASSERT_VALUES_EQUAL(delivered->load(), id.Generation);
    UNIT_ASSERT(provider->Calls.front().Request.expired());
}

Y_UNIT_TEST(DrainRearmsWhilePreviousWakeupIsStillInFlight) {
    std::barrier entered(2);
    std::barrier resume(2);
    std::atomic<size_t> notifications{0};
    auto provider = std::make_shared<TManualProvider>();
    TExecutionContext context([&](ui64) {
        if (++notifications == 1) {
            entered.arrive_and_wait();
            resume.arrive_and_wait();
        }
    });
    context.RegisterProvider("test", provider);
    context.Submit("test", {"echo", {}, 16, {}});
    context.Submit("test", {"echo", {}, 16, {}});
    const auto first = provider->Calls[0].Completion;
    std::thread producer([first] { first->Complete("first"); });
    entered.arrive_and_wait();
    const auto firstResults = context.TakeReady();
    provider->Calls[1].Completion->Complete("second");
    const auto secondResults = context.TakeReady();
    resume.arrive_and_wait();
    producer.join();
    UNIT_ASSERT_VALUES_EQUAL(notifications.load(), 2);
    UNIT_ASSERT_VALUES_EQUAL(firstResults.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(secondResults.size(), 1);
    UNIT_ASSERT(firstResults.front().Id != secondResults.front().Id);
}

Y_UNIT_TEST(OldGenerationCannotCancelNewOperation) {
    TFixture oldTask;
    const auto old = oldTask.Submit().Id;
    oldTask.Context.Shutdown();
    TFixture newTask;
    const auto current = newTask.Submit().Id;
    UNIT_ASSERT_VALUES_EQUAL(old.Sequence, current.Sequence);
    UNIT_ASSERT(old.Generation != current.Generation);
    UNIT_ASSERT(!newTask.Context.Cancel(old));
    oldTask.Provider->Calls.front().Completion->Complete("stale");
    oldTask.Provider->Calls.front().Completion->Release();
    UNIT_ASSERT(newTask.Context.TakeReady().empty());
    newTask.Provider->Calls.front().Completion->Complete("current");
    UNIT_ASSERT_VALUES_EQUAL(newTask.TakeOne().Payload, "current");
}

Y_UNIT_TEST(DeadlineIsEventDrivenAndCancelIsMonotonic) {
    TFixture f;
    const auto id = f.Submit(At(10)).Id;
    UNIT_ASSERT(f.Context.NextDeadline() == At(10));
    f.Tick->store(9);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.ExpireDeadlines(), 0);
    f.Tick->store(10);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.ExpireDeadlines(), 1);
    UNIT_ASSERT(!f.Context.NextDeadline());
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Timeout);
    UNIT_ASSERT(!f.Context.Cancel(id));
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.front().Cancellations, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().CleanupPending, 1);
    f.Provider->Calls.front().Completion->Release();
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
}

Y_UNIT_TEST(LateCompletionTimesOutWithoutTimerDispatch) {
    TFixture f;
    f.Submit(At(10));
    f.Tick->store(10);
    f.Provider->Calls.front().Completion->Complete("late");
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Timeout);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.ExpireDeadlines(), 0);
}

Y_UNIT_TEST(CancellationBeforeDeadlineDoesNotBecomeTimeoutDuringCleanup) {
    TFixture f;
    const auto id = f.Submit(At(10)).Id;
    f.Tick->store(9);
    f.Context.Cancel(id);
    f.Tick->store(11);
    f.Provider->Calls.front().Completion->Release();
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Cancelled);
    const auto next = f.Submit(At(20)).Id;
    f.Tick->store(20);
    f.Context.Cancel(next);
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Timeout);
}

Y_UNIT_TEST(SuccessBeforeDeadlineSurvivesLateConsumptionAndShutdown) {
    TFixture f;
    f.Submit(At(10));
    f.Provider->Calls.front().Completion->Complete("early");
    f.Tick->store(20);
    f.Context.Shutdown();
    UNIT_ASSERT(f.TakeOne().Outcome == EOutcome::Success);
}

Y_UNIT_TEST(AdmissionFailuresDoNotLaunchProvider) {
    TFixture f({8, 25});
    UNIT_ASSERT(f.Context.Submit("missing", {}).Status == EAdmission::UnknownProvider);
    UNIT_ASSERT(f.Submit(At(0)).Status == EAdmission::DeadlineExceeded);
    UNIT_ASSERT(f.Context.Submit("test", {"echo", "input", 17, {}}).Status == EAdmission::BytesLimit);
    UNIT_ASSERT(f.Context.Submit("test", {"echo", "input", std::numeric_limits<ui64>::max(), {}}).Status == EAdmission::BytesLimit);
    UNIT_ASSERT(f.Provider->Calls.empty());
    f.Submit();
    UNIT_ASSERT(f.Submit().Status == EAdmission::BytesLimit);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 25);
}

Y_UNIT_TEST(ResponseLimitAppliesToSuccessAndFailure) {
    TFixture f;
    f.Submit();
    f.Provider->Calls.back().Completion->Complete(TString(17, 'x'));
    auto result = f.TakeOne();
    UNIT_ASSERT(result.Outcome == EOutcome::Error);
    UNIT_ASSERT(result.Error == EError::ResponseTooLarge);
    UNIT_ASSERT(result.Payload.empty());
    f.Submit();
    f.Provider->Calls.back().Completion->Fail(TString(17, 'x'));
    UNIT_ASSERT(f.TakeOne().Error == EError::ResponseTooLarge);
}

Y_UNIT_TEST(ProviderRegistrationAndRouting) {
    TFixture f;
    UNIT_ASSERT_EXCEPTION(f.Context.RegisterProvider("test", f.Provider), yexception);
    auto other = std::make_shared<TManualProvider>();
    f.Context.RegisterProvider("other", other);
    const auto id = f.Context.Submit("other", {"echo", {}, 1, {}}).Id;
    f.Context.Cancel(id);
    UNIT_ASSERT(f.Provider->Calls.empty());
    UNIT_ASSERT_VALUES_EQUAL(other->Calls.front().Cancellations, 1);
    f.Context.Shutdown();
    UNIT_ASSERT_EXCEPTION(f.Context.RegisterProvider("closed", other), yexception);
}

Y_UNIT_TEST(WakeupRunsOutsideRegistryLock) {
    auto provider = std::make_shared<TManualProvider>();
    TExecutionContext* observer = nullptr;
    size_t observed = 0;
    TExecutionContext context([&](ui64) {
        // Observation is thread-safe; no computation or owner method reentry.
        observed = observer->GetStats().ReadyResults;
    });
    observer = &context;
    context.RegisterProvider("test", provider);
    context.Submit("test", {"echo", {}, 1, {}});
    provider->Calls.front().Completion->Complete("x");
    UNIT_ASSERT_VALUES_EQUAL(observed, 1);
}

Y_UNIT_TEST(CompletionRacingCancelHasExactlyOneOutcome) {
    TFixture f;
    for (size_t i = 0; i < 200; ++i) {
        const auto id = f.Submit().Id;
        const auto token = f.Provider->Calls.back().Completion;
        std::barrier gate(2);
        std::thread producer([&] {
            gate.arrive_and_wait();
            token->Complete("ok");
            token->Release();
        });
        gate.arrive_and_wait();
        const bool cancelled = f.Context.Cancel(id);
        producer.join();
        const auto result = f.TakeOne();
        UNIT_ASSERT(result.Id == id);
        UNIT_ASSERT(result.Outcome == (cancelled ? EOutcome::Cancelled : EOutcome::Success));
        UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
    }
}

Y_UNIT_TEST(DrainRacingCompletionAlwaysRearmsNotifications) {
    TFixture f;
    for (size_t i = 0; i < 200; ++i) {
        f.Submit();
        const auto token = f.Provider->Calls.back().Completion;
        const auto before = f.Wakeups->load();
        std::barrier gate(2);
        std::thread producer([&] {
            gate.arrive_and_wait();
            token->Complete("ready");
            token->Release();
        });
        gate.arrive_and_wait();
        auto first = f.Context.TakeReady();
        producer.join();
        auto second = f.Context.TakeReady();
        UNIT_ASSERT_VALUES_EQUAL(first.size() + second.size(), 1);
        // If the owner drained the result before Release, returning credits
        // requires a second notification. Both paths are bounded and useful.
        UNIT_ASSERT(f.Wakeups->load() >= before + 1);
        UNIT_ASSERT(f.Wakeups->load() <= before + 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
    }
}

Y_UNIT_TEST(ParallelCompletionsAreCorrelatedAndCoalesced) {
    TFixture f;
    for (size_t i = 0; i < 64; ++i) {
        f.Submit();
    }
    std::vector<std::thread> producers;
    for (size_t worker = 0; worker < 4; ++worker) {
        producers.emplace_back([&, worker] {
            for (size_t i = worker; i < 64; i += 4) {
                auto& call = f.Provider->Calls[i];
                call.Completion->Complete(ToString(i));
                call.Completion->Release();
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    auto results = f.Context.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 64);
    UNIT_ASSERT_VALUES_EQUAL(f.Wakeups->load(), 1);
    for (const auto& result : results) {
        UNIT_ASSERT_VALUES_EQUAL(result.Payload, ToString(result.Id.Sequence - 1));
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().ReservedPayloadBytes, 0);
}

Y_UNIT_TEST(ShutdownRacingCompletionsPreservesSingleTerminalResult) {
    TFixture f;
    for (size_t i = 0; i < 64; ++i) {
        f.Submit();
    }
    std::barrier gate(2);
    std::thread producer([&] {
        gate.arrive_and_wait();
        for (auto& call : f.Provider->Calls) {
            call.Completion->Complete("ok");
            call.Completion->Release();
        }
    });
    gate.arrive_and_wait();
    f.Context.Shutdown();
    producer.join();
    const auto results = f.Context.TakeReady();
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 64);
    for (const auto& result : results) {
        UNIT_ASSERT(result.Outcome == EOutcome::Success || result.Outcome == EOutcome::Cancelled);
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Context.GetStats().TrackedOperations, 0);
    UNIT_ASSERT(f.Submit().Status == EAdmission::Closed);
}
} // Y_UNIT_TEST_SUITE(TAsyncOperationLifecycle)
