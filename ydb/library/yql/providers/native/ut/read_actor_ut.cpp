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

class TStream final : public IReadStream {
public:
    NThreading::TFuture<TReadResult> Next() override {
        auto promise = NThreading::NewPromise<TReadResult>();
        {
            std::lock_guard lock(Mutex);
            UNIT_ASSERT(!Pending);
            Pending = promise;
        }
        if (BeforeNotification) {
            promise.GetFuture().Subscribe([before = BeforeNotification](const auto&) { before(); });
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

    std::function<void()> BeforeNotification;
    std::atomic<ui32> Calls = 0;
    std::atomic<bool> Cancelled = false;
    NThreading::TPromise<void> Started = NThreading::NewPromise();
private:
    std::mutex Mutex;
    std::optional<NThreading::TPromise<TReadResult>> Pending;
};

void Init(TFakeCASetup& setup, TReadStreamFactory factory,
          TDuration timeout = TDuration::Seconds(30), bool pollBeforeBootstrap = false) {
    setup.Execute([&](TFakeActor& actor) {
        NDqProto::TTaskInput input;
        THashMap<TString, TString> params;
        TVector<TString> ranges;
        auto [asyncInput, readActor] = CreateNativeReadActor(std::move(factory),
            {.Timeout = timeout, .MaxBatchBytes = 1024, .MaxRetries = 2, .Columns = {"value"}},
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
    Y_UNIT_TEST(UndeliverableEventMayReleasePayloadReentrantly) {
        struct TEvOwnPayload : NActors::TEventLocal<TEvOwnPayload, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100> {
            explicit TEvOwnPayload(std::shared_ptr<void> payload) : Payload(std::move(payload)) {}
            std::shared_ptr<void> Payload;
        };
        TFakeCASetup setup;
        bool released = false;
        setup.Execute([&](TFakeActor& actor) {
            auto mailbox = std::make_shared<TCallbackMailbox>(NActors::TActivationContext::ActorSystem(),
                NActors::TActorId(actor.SelfId().NodeId(), "missing"));
            auto payload = std::shared_ptr<void>(new int, [mailbox, &released](void* value) {
                released = true;
                mailbox->Send(new NActors::TEvents::TEvWakeup());
                delete static_cast<int*>(value);
            });
            mailbox->Send(new TEvOwnPayload(std::move(payload)));
            mailbox->Detach();
        });
        UNIT_ASSERT(released);
    }

    Y_UNIT_TEST(InitialPollBeforeBootstrapWaitsForInitialization) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Init(setup, [stream](const auto&) { return stream; }, TDuration::Seconds(30), true);
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
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return std::make_shared<TStream>(); });
        setup.Terminate();
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 0);
    }

    Y_UNIT_TEST(PositiveDemandBoundsOneReadAndOneBatch) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        Init(setup, [stream](const auto&) { return stream; });
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, 0).Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(Pull(setup, -1).Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 0);
        auto next = Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
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
    }

    Y_UNIT_TEST(LocalDeadlineIsNotRestartedByRetry) {
        TFakeCASetup setup;
        auto first = std::make_shared<TStream>();
        auto second = std::make_shared<TStream>();
        std::atomic<ui32> attempts = 0;
        TInstant deadline;
        Init(setup, [&](const auto& context) {
            if (++attempts == 1) {
                deadline = context.Deadline;
                UNIT_ASSERT(deadline > TInstant::Now());
                return first;
            }
            UNIT_ASSERT_VALUES_EQUAL(context.Deadline, deadline);
            return second;
        }, TDuration::Seconds(5));
        Pull(setup, 1);
        UNIT_ASSERT(first->Started.GetFuture().Wait(WaitTimeout));
        first->Resolve({.Error = "temporary", .Retryable = true});
        UNIT_ASSERT(second->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(first->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(attempts.load(), 2);
        setup.Terminate();
    }

    Y_UNIT_TEST(FailureAfterDeliveryCannotOpenAnotherSnapshot) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        std::atomic<ui32> attempts = 0;
        Init(setup, [&](const auto&) { ++attempts; return stream; });
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
        Init(setup, [stream](const auto&) { return stream; }, TDuration::MilliSeconds(200));
        auto error = setup.AsyncInputPromises->FatalError.GetFuture();
        Pull(setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT(error.Wait(WaitTimeout));
        UNIT_ASSERT(stream->Cancelled);
        UNIT_ASSERT_VALUES_EQUAL(stream->Calls.load(), 1);
        setup.Terminate();
    }

    Y_UNIT_TEST(LateCallbackAfterActorSystemDestructionIsSafe) {
        auto setup = std::make_unique<TFakeCASetup>();
        auto stream = std::make_shared<TStream>();
        auto callbackStarted = NThreading::NewPromise();
        auto continueCallback = NThreading::NewPromise();
        stream->BeforeNotification = [callbackStarted, resume = continueCallback.GetFuture()]() mutable {
            callbackStarted.TrySetValue();
            resume.Wait();
        };
        Init(*setup, [stream](const auto&) { return stream; });
        Pull(*setup, 1);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        // SetValue has completed the future, but the provider subscriber is
        // deliberately delayed behind another subscriber until after shutdown.
        std::thread callback([stream] { stream->Resolve(Batch(42)); });
        const bool started = callbackStarted.GetFuture().Wait(WaitTimeout);
        if (!started) {
            continueCallback.TrySetValue(); // Allow failure cleanup to finish too.
        }
        setup.reset();
        continueCallback.TrySetValue();
        callback.join();
        UNIT_ASSERT(started);
        UNIT_ASSERT(stream->Cancelled);
    }

    Y_UNIT_TEST(OnlyEofCompletesTheInput) {
        TFakeCASetup setup;
        auto stream = std::make_shared<TStream>();
        Init(setup, [stream](const auto&) { return stream; });
        auto first = Pull(setup, 1);
        UNIT_ASSERT(!first.Finished);
        UNIT_ASSERT(stream->Started.GetFuture().Wait(WaitTimeout));
        stream->Resolve({.Finished = true});
        UNIT_ASSERT(first.Notification.Wait(WaitTimeout));
        UNIT_ASSERT(Pull(setup, 0).Finished);
    }
}

} // namespace NYql::NNative
