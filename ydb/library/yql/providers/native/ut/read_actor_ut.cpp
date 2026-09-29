#include <ydb/library/yql/providers/native/read_stream.h>
#include <ydb/library/yql/providers/native/actors/callback_mailbox.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <arrow/api.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <thread>

namespace NYql::NNative {
namespace {

using namespace NDq;
const auto WaitTimeout = TDuration::Seconds(10);

class TQuota final : public IMemoryQuotaManager {
public:
    bool AllocateQuota(ui64 bytes, bool isOptional) override {
        UNIT_ASSERT(NActors::TlsActivationContext);
        UNIT_ASSERT(isOptional);
        if (++Attempts == 1) {
            Attempted.SetValue();
        }
        if (Reject.load()) {
            return false;
        }
        Allocated += bytes;
        Granted.TrySetValue();
        return true;
    }
    void FreeQuota(ui64 bytes) override {
        UNIT_ASSERT(NActors::TlsActivationContext);
        Allocated -= bytes;
        Released.TrySetValue();
    }
    ui64 GetCurrentQuota() const override { return Allocated; }
    ui64 GetMaxMemorySize() const override { return Allocated; }
    i64 GetMemoryAvailability() const override { return 1000000; }
    TString MemoryConsumptionDetails() const override { return {}; }
    std::atomic<ui64> Allocated = 0;
    std::atomic<bool> Reject = false;
    std::atomic<ui64> Attempts = 0;
    NThreading::TPromise<void> Attempted = NThreading::NewPromise();
    NThreading::TPromise<void> Granted = NThreading::NewPromise();
    NThreading::TPromise<void> Released = NThreading::NewPromise();
};

class TStream final : public IReadStream {
public:
    NThreading::TFuture<TReadResult> Next() override {
        auto promise = NThreading::NewPromise<TReadResult>();
        {
            std::lock_guard lock(Mutex);
            UNIT_ASSERT(!Pending);
            Pending = promise;
        }
        if (++Calls == 1) {
            Started.SetValue();
        }
        return promise.GetFuture();
    }

    void Resolve(TReadResult result) {
        NThreading::TPromise<TReadResult> promise;
        {
            std::lock_guard lock(Mutex);
            UNIT_ASSERT(Pending);
            promise = *Pending;
            Pending.reset();
        }
        promise.SetValue(std::move(result));
    }

    void Cancel() override {
        if (Cancelled.exchange(true)) {
            return;
        }
        std::optional<NThreading::TPromise<TReadResult>> promise;
        {
            std::lock_guard lock(Mutex);
            promise.swap(Pending);
        }
        if (promise) {
            promise->SetValue({.Error = "cancelled"});
        }
    }

    std::atomic<ui32> Calls = 0;
    std::atomic<bool> Cancelled = false;
    NThreading::TPromise<void> Started = NThreading::NewPromise();
private:
    std::mutex Mutex;
    std::optional<NThreading::TPromise<TReadResult>> Pending;
};

void Init(TFakeCASetup& setup, TReadStreamFactory factory, const std::shared_ptr<TQuota>& quota,
          TDuration timeout = TDuration::Seconds(30), bool pollBeforeBootstrap = false,
          TInstant queryDeadline = TInstant::Max()) {
    setup.Execute([&](TFakeActor& actor) {
        NDqProto::TTaskInput input;
        THashMap<TString, TString> params;
        TVector<TString> ranges;
        auto [asyncInput, readActor] = CreateNativeReadActor(std::move(factory),
            {.Timeout = timeout, .MaxBatchBytes = 1024, .MemoryReservation = 4096, .MaxRetries = 2, .Columns = {"value"}},
            IDqAsyncIoFactory::TSourceArguments{
                .InputDesc = input,
                .InputIndex = 0,
                .StatsLevel = {},
                .TxId = {},
                .TaskId = 1,
                .SecureParams = params,
                .TaskParams = params,
                .ReadRanges = ranges,
                .ComputeActorId = actor.SelfId(),
                .TypeEnv = actor.TypeEnv,
                .HolderFactory = actor.HolderFactory,
                .ProgramBuilder = actor.ProgramBuilder,
                .MemoryQuotaManager = quota,
                .Deadline = queryDeadline,
            });
        actor.InitAsyncInput(asyncInput, readActor);
        if (pollBeforeBootstrap) {
            // Reproduce the CA's synchronous initial poll. The child shares this
            // mailbox, so its Bootstrap event cannot run until this callback returns.
            NKikimr::NMiniKQL::TUnboxedValueBatch batch;
            TMaybe<TInstant> watermark;
            bool finished = false;
            UNIT_ASSERT_VALUES_EQUAL(asyncInput->GetAsyncInputData(batch, watermark, finished, 1024), 0);
            UNIT_ASSERT_VALUES_EQUAL(batch.RowCount(), 0);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
        }
    });
    setup.Execute([](TFakeActor&) {}); // Drain bootstrap before capturing notification promises.
}

struct TPull {
    ui64 Rows = 0;
    i64 Bytes = 0;
    bool Finished = false;
    NThreading::TFuture<void> Notification;
};

TPull Pull(TFakeCASetup& setup, i64 freeSpace) {
    TPull result;
    setup.Execute([&](TFakeActor& actor) {
        NKikimr::NMiniKQL::TUnboxedValueBatch batch;
        TMaybe<TInstant> watermark;
        result.Bytes = actor.DqAsyncInput->GetAsyncInputData(batch, watermark, result.Finished, freeSpace);
        result.Rows = batch.RowCount();
        result.Notification = setup.AsyncInputPromises->NewAsyncInputDataArrived.GetFuture();
    });
    return result;
}

TReadResult Batch(ui64 value) {
    arrow::UInt64Builder builder;
    UNIT_ASSERT(builder.Append(value).ok());
    auto array = builder.Finish().ValueOrDie();
    return {.Batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::uint64())}), 1, {array}), .Bytes = 8};
}

} // namespace

Y_UNIT_TEST_SUITE(NativeReadActor) {
    Y_UNIT_TEST(UndeliverableEventMayReleaseLeaseReentrantly) {
        struct TEvOwnLease : NActors::TEventLocal<TEvOwnLease, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100> {
            explicit TEvOwnLease(std::shared_ptr<void> lease) : Lease(std::move(lease)) {}
            std::shared_ptr<void> Lease;
        };
        TFakeCASetup setup;
        bool released = false;
        setup.Execute([&](TFakeActor& actor) {
            auto mailbox = std::make_shared<TCallbackMailbox>(NActors::TActivationContext::ActorSystem(),
                NActors::TActorId(actor.SelfId().NodeId(), "missing"));
            auto lease = std::shared_ptr<void>(new int, [mailbox, &released](void* value) {
                released = true;
                mailbox->Send(new NActors::TEvents::TEvWakeup());
                delete static_cast<int*>(value);
            });
            mailbox->Send(new TEvOwnLease(std::move(lease)));
            mailbox->Detach();
        });
        UNIT_ASSERT(released);
    }

    Y_UNIT_TEST(InitialPollBeforeBootstrapWaitsForInitialization) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Init(setup, [stream](const auto&) { return stream; }, quota, TDuration::Seconds(30), true);
        UNIT_ASSERT(!error.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 0);
        auto first = Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        stream->Resolve({.Finished = true});
        UNIT_ASSERT(first.Notification.Wait(WaitTimeout));
        UNIT_ASSERT(Pull(setup, 0).Finished);
        UNIT_ASSERT(!error.HasValue());
    }

    Y_UNIT_TEST(CancellationBeforeDemandDoesNotStartRemoteOperation) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return std::make_shared<TStream>(); }, quota);
        setup.Terminate();
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
    }

    Y_UNIT_TEST(PositiveDemandBoundsOneReadAndOneBatch) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        Init(setup, [stream](const auto&) { return stream; }, quota);
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, 0).Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, -1).Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
        auto next = Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 4096);
        Pull(setup, 1);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        stream->Resolve(Batch(42));
        UNIT_ASSERT(next.Notification.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, 0).Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        auto delivered = Pull(setup, 1);
        UNIT_ASSERT_VALUES_EQUAL(delivered.Rows, 1);
        UNIT_ASSERT_VALUES_EQUAL(delivered.Bytes, 8);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        Pull(setup, 1);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 2);
        setup.Terminate();
        UNIT_ASSERT(stream->Cancelled);
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
    }

    Y_UNIT_TEST(SourceDeadlineIsNotRestartedByAdmissionOrRetry) {
        TFakeCASetup setup;
        auto first = std::make_shared<TStream>();
        auto second = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        quota->Reject = true;
        std::atomic<ui32> attempts = 0;
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        Init(setup, [&](const auto& context) {
            UNIT_ASSERT_VALUES_EQUAL(context.Deadline, deadline);
            if (++attempts == 1) {
                return first;
            }
            return second;
        }, quota, TDuration::Seconds(60), false, deadline);
        Pull(setup, 1);
        UNIT_ASSERT(quota->Attempted.GetFuture().Wait(WaitTimeout));
        quota->Reject = false;
        UNIT_ASSERT(first->Started.GetFuture().Wait(WaitTimeout));
        first->Resolve({.Error = "temporary", .Retryable = true});
        UNIT_ASSERT(second->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(first->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 2);
        setup.Terminate();
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
    }

    Y_UNIT_TEST(FailureAfterDeliveryCannotOpenAnotherSnapshot) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return stream; }, quota);
        auto first = Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        stream->Resolve(Batch(42));
        UNIT_ASSERT(first.Notification.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, 1).Rows, 1);
        Pull(setup, 1);
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        stream->Resolve({.Error = "temporary", .Retryable = true});
        UNIT_ASSERT(error.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 1);
        UNIT_ASSERT(stream->Cancelled);
    }

    Y_UNIT_TEST(DeadlineCancelsPendingRead) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        Init(setup, [stream](const auto&) { return stream; }, quota, TDuration::MilliSeconds(200));
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(error.Wait(WaitTimeout));
        UNIT_ASSERT(stream->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        setup.Terminate();
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
    }

    Y_UNIT_TEST(CancellationWhileWaitingForQuotaPreventsRemoteOperation) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        quota->Reject = true;
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return std::make_shared<TStream>(); }, quota);
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Pull(setup, 1);
        UNIT_ASSERT(quota->Attempted.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(!error.HasValue());
        setup.Terminate();
        quota->Reject = false;
        setup.Execute([](TFakeActor&) {});
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
    }

    Y_UNIT_TEST(QuotaAdmissionResumesWhenResourcesBecomeAvailable) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        quota->Reject = true;
        auto stream = std::make_shared<TStream>();
        Init(setup, [stream](const auto&) { return stream; }, quota);
        auto first = Pull(setup, 1);
        UNIT_ASSERT(quota->Attempted.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(!stream->Started.HasValue());
        quota->Reject = false;
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        stream->Resolve({.Finished = true});
        UNIT_ASSERT(first.Notification.Wait(WaitTimeout));
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
    }

    Y_UNIT_TEST(QuotaGrantDoesNotStartReadAfterDemandIsRevoked) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        quota->Reject = true;
        auto stream = std::make_shared<TStream>();
        Init(setup, [stream](const auto&) { return stream; }, quota);
        Pull(setup, 1);
        UNIT_ASSERT(quota->Attempted.GetFuture().Wait(WaitTimeout));
        Pull(setup, 0);
        quota->Reject = false;
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 0);
        Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        setup.Terminate();
    }

    Y_UNIT_TEST(AbsoluteQueryDeadlineIncludesQuotaWait) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        quota->Reject = true;
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return std::make_shared<TStream>(); }, quota,
            TDuration::Seconds(30), false, TInstant::Now() + TDuration::MilliSeconds(200));
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Pull(setup, 1);
        UNIT_ASSERT(quota->Attempted.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(error.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 0);
    }

    Y_UNIT_TEST(QuotaSurvivesSourceUntilLateCallbackLeaseIsReleased) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        auto stream = std::make_shared<TStream>();
        std::shared_ptr<void> sdkCallbackLease;
        Init(setup, [&](const auto& context) {
            sdkCallbackLease = context.MemoryLease;
            return stream;
        }, quota);
        auto notification = Pull(setup, 1).Notification;
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        auto exitCallback = NThreading::NewPromise();
        // The SDK invokes the subscriber synchronously before its callback stack
        // unwinds. Fulfil the read first, and deliberately retain that stack.
        std::thread callback([stream, lease = std::move(sdkCallbackLease), exit = exitCallback.GetFuture()]() mutable {
            stream->Resolve(Batch(42));
            exit.Wait();
            lease.reset();
        });
        const bool delivered = notification.Wait(WaitTimeout);
        setup.Terminate();
        const auto allocatedWhileCallbackRuns = quota->Allocated.load();
        const bool releasedWhileCallbackRuns = quota->Released.HasValue();
        exitCallback.SetValue();
        callback.join();
        UNIT_ASSERT(delivered);
        UNIT_ASSERT(stream->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(allocatedWhileCallbackRuns, 4096);
        UNIT_ASSERT(!releasedWhileCallbackRuns);
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
    }

    Y_UNIT_TEST(LateLeaseReleaseAfterActorSystemDestructionIsSafe) {
        auto setup = std::make_unique<TFakeCASetup>();
        auto quota = std::make_shared<TQuota>();
        std::weak_ptr<TQuota> quotaLifetime = quota;
        auto stream = std::make_shared<TStream>();
        std::shared_ptr<void> sdkCallbackLease;
        Init(*setup, [&](const auto& context) {
            sdkCallbackLease = context.MemoryLease;
            return stream;
        }, quota);
        Pull(*setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        setup.reset();
        quota.reset();
        UNIT_ASSERT(!quotaLifetime.expired());
        std::thread callback([lease = std::move(sdkCallbackLease)]() mutable { lease.reset(); });
        callback.join();
        UNIT_ASSERT(quotaLifetime.expired());
    }

    Y_UNIT_TEST(RetryWaitsForPreviousCallbackQuiescence) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        auto first = std::make_shared<TStream>();
        auto second = std::make_shared<TStream>();
        std::shared_ptr<void> sdkCallbackLease;
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto& context) {
            if (++attempts == 1) {
                sdkCallbackLease = context.MemoryLease;
                return first;
            }
            return second;
        }, quota);
        Pull(setup, 1);
        UNIT_ASSERT(first->Started.GetFuture().Wait(WaitTimeout));
        first->Resolve({.Error = "temporary", .Retryable = true});
        UNIT_ASSERT(!second->Started.GetFuture().Wait(TDuration::MilliSeconds(300)));
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 1);
        sdkCallbackLease.reset();
        UNIT_ASSERT(second->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 2);
        setup.Terminate();
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
    }

    Y_UNIT_TEST(GovernorShutdownCancelsAdmissionAndRetainsLiveLease) {
        TFakeCASetup setup;
        auto quota = std::make_shared<TQuota>();
        std::shared_ptr<IAsyncMemoryQuota> governor;
        setup.Execute([&](TFakeActor& actor) {
            governor = CreateAsyncMemoryQuota(NActors::TActivationContext::ActorSystem(), quota, 4096,
                [&](NActors::IActor* child) {
                    return NActors::TActivationContext::RegisterWithSameMailbox(child, actor.SelfId());
                });
        });
        NThreading::TCancellationTokenSource cancellation;
        auto deadline = TInstant::Now() + TDuration::Seconds(30);
        auto granted = governor->Acquire(4096, deadline, cancellation.Token());
        UNIT_ASSERT(granted.Wait(WaitTimeout));
        auto lease = granted.ExtractValueSync();
        auto pending = governor->Acquire(4096, deadline, cancellation.Token());
        governor->Shutdown();
        UNIT_ASSERT(pending.Wait(WaitTimeout));
        UNIT_ASSERT_EXCEPTION(pending.GetValue(), yexception);
        governor.reset();
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 4096);
        std::thread owner([lease = std::move(lease)]() mutable { lease.reset(); });
        owner.join();
        UNIT_ASSERT(quota->Released.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(quota->Allocated.load(), 0);
    }

    Y_UNIT_TEST(OnlyEofCompletesTheInput) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto quota = std::make_shared<TQuota>();
        Init(setup, [stream](const auto&) { return stream; }, quota);
        auto first = Pull(setup, 1);
        UNIT_ASSERT(!first.Finished);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        stream->Resolve({.Finished = true});
        UNIT_ASSERT(first.Notification.Wait(WaitTimeout));
        UNIT_ASSERT(Pull(setup, 0).Finished);
    }
}

} // namespace NYql::NNative
