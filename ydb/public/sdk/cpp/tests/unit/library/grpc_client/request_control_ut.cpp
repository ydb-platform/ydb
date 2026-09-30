#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/request_control.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <barrier>
#include <stdexcept>
#include <thread>

namespace NYdbGrpc::inline Dev {

    struct TStreamRequestReadProcessorTestAccess {
        template <typename TProcessor>
        static void SetStream(TProcessor& processor, typename TProcessor::TAsyncReaderPtr stream, bool started) {
            processor.Stream = std::move(stream);
            processor.Started = started;
            if (started) {
                processor.Callback = nullptr;
            }
        }

        template <typename TProcessor>
        static void SetLifetime(TProcessor& processor, std::shared_ptr<void> lifetime) {
            TCallMeta meta;
            meta.RequestLifetime = std::move(lifetime);
            processor.ApplyMeta(meta);
        }

        template <typename TProcessor>
        static void CompleteStart(TProcessor& processor, bool ok) {
            processor.OnStartDone(ok);
        }

        template <typename TProcessor>
        static void SetDecodeError(TProcessor& processor) {
            processor.BoundedDecodeStatus_ = grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "response rejected");
        }
    };

} // namespace NYdbGrpc::inline Dev

namespace {

    struct TMessage {};
    struct TStub {};
    using TProcessor = NYdbGrpc::TStreamRequestReadProcessor<TStub, TMessage, TMessage>;

    class TReader final: public grpc::ClientAsyncReaderInterface<TMessage> {
    public:
        explicit TReader(bool& destroyed)
            : Destroyed_(destroyed)
        {
        }

        ~TReader() override {
            Destroyed_ = true;
        }

        void StartCall(void*) override {
        }
        void ReadInitialMetadata(void*) override {
        }

        void Read(TMessage*, void* tag) override {
            ReadTag_ = tag;
        }

        void Finish(grpc::Status* status, void* tag) override {
            UNIT_ASSERT(!FinishTag_);
            FinishStatus_ = status;
            FinishTag_ = tag;
            ++FinishCalls;
        }

        void CompleteRead(bool ok) {
            Complete(ReadTag_, ok);
        }

        void CompleteFinish() {
            *FinishStatus_ = grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
            Complete(FinishTag_, true);
        }

        unsigned FinishCalls = 0;

    private:
        static void Complete(void*& tag, bool ok) {
            auto* event = static_cast<NYdbGrpc::IQueueClientEvent*>(std::exchange(tag, nullptr));
            UNIT_ASSERT(event);
            event->Execute(ok);
            event->Destroy();
        }

        bool& Destroyed_;
        void* ReadTag_ = nullptr;
        void* FinishTag_ = nullptr;
        grpc::Status* FinishStatus_ = nullptr;
    };

    struct TLease {
        TLease(bool& destroyed, bool& readerDestroyed)
            : Destroyed(destroyed)
            , ReaderDestroyed(readerDestroyed)
        {
        }

        ~TLease() {
            UNIT_ASSERT(ReaderDestroyed);
            Destroyed = true;
        }

        bool& Destroyed;
        bool& ReaderDestroyed;
    };

} // namespace

Y_UNIT_TEST_SUITE(RequestControlTests) {
    Y_UNIT_TEST(CancelBeforeSubscriptionAndRepeatedCancel) {
        auto control = std::make_shared<NYdb::TRequestControl>();
        control->Cancel();
        size_t calls = 0;
        auto registration = NYdb::TRequestControlAccess::Subscribe(control, [&] {
            UNIT_ASSERT(control->IsCancelled()); // No internal lock during callback.
            ++calls;
        });
        control->Cancel();
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        UNIT_ASSERT(!registration);
    }

    Y_UNIT_TEST(CompletedSubscriptionsAreReleasedAndFailuresDoNotStopCancellation) {
        auto control = std::make_shared<NYdb::TRequestControl>();
        size_t calls = 0;
        for (size_t i = 0; i < 10000; ++i) {
            auto capture = std::make_shared<int>(1);
            std::weak_ptr<int> weak = capture;
            auto registration = NYdb::TRequestControlAccess::Subscribe(control, [capture] {});
            capture.reset();
            registration.reset();
            UNIT_ASSERT(weak.expired());
        }
        auto first = NYdb::TRequestControlAccess::Subscribe(control, [] { throw std::runtime_error("transport failure"); });
        auto second = NYdb::TRequestControlAccess::Subscribe(control, [&] { ++calls; });
        control->Cancel();
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
    }

    Y_UNIT_TEST(ConcurrentSubscriptionAndCancellationCompleteExactlyOnce) {
        for (size_t attempt = 0; attempt < 64; ++attempt) {
            auto control = std::make_shared<NYdb::TRequestControl>();
            std::atomic<unsigned> calls = 0;
            std::barrier start(2);
            std::shared_ptr<void> registration;
            std::thread subscriber([&] {
                start.arrive_and_wait();
                registration = NYdb::TRequestControlAccess::Subscribe(control, [&] { ++calls; });
            });
            start.arrive_and_wait();
            control->Cancel();
            subscriber.join();
            UNIT_ASSERT_VALUES_EQUAL(calls.load(), 1);
        }
    }

    Y_UNIT_TEST(CancellationBeforeStreamStartCancelsInitialCallbackOnce) {
        bool readerDestroyed = false;
        unsigned replies = 0;
        auto processor = MakeIntrusive<TProcessor>([&](NYdbGrpc::TGrpcStatus&& status, auto stream) {
            UNIT_ASSERT_VALUES_EQUAL(status.GRpcStatusCode, static_cast<int>(grpc::StatusCode::CANCELLED));
            UNIT_ASSERT(!stream);
            ++replies;
        });
        auto reader = std::make_unique<TReader>(readerDestroyed);
        auto* raw = reader.get();
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetStream(*processor, std::move(reader), false);
        processor->Cancel();
        processor->Cancel();
        UNIT_ASSERT_VALUES_EQUAL(raw->FinishCalls, 0);
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::CompleteStart(*processor, true);
        UNIT_ASSERT_VALUES_EQUAL(raw->FinishCalls, 1);
        raw->CompleteFinish();
        processor->Cancel();
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
    }

    Y_UNIT_TEST(LifetimeOutlivesCancelledReadCallbackAndTransportDestruction) {
        bool readerDestroyed = false;
        bool leaseDestroyed = false;
        unsigned replies = 0;
        auto processor = MakeIntrusive<TProcessor>([](NYdbGrpc::TGrpcStatus&&, auto) {});
        auto reader = std::make_unique<TReader>(readerDestroyed);
        auto* raw = reader.get();
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetStream(*processor, std::move(reader), true);
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetLifetime(*processor,
                                                                     std::make_shared<TLease>(leaseDestroyed, readerDestroyed));
        TMessage message;
        processor->Read(&message, [&](NYdbGrpc::TGrpcStatus&& status) {
            UNIT_ASSERT_VALUES_EQUAL(status.GRpcStatusCode, static_cast<int>(grpc::StatusCode::CANCELLED));
            UNIT_ASSERT(!leaseDestroyed);
            ++replies;
            processor.Reset();
            UNIT_ASSERT(!leaseDestroyed); // The active completion event still owns transport.
        });
        processor->Cancel();
        raw->CompleteRead(false);
        UNIT_ASSERT(!leaseDestroyed);
        raw->CompleteFinish();
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        UNIT_ASSERT(readerDestroyed);
        UNIT_ASSERT(leaseDestroyed);
    }

    Y_UNIT_TEST(DecoderFailureSurvivesTransportCancellationAndRetainsLifetime) {
        bool readerDestroyed = false;
        bool leaseDestroyed = false;
        unsigned replies = 0;
        auto processor = MakeIntrusive<TProcessor>([](NYdbGrpc::TGrpcStatus&&, auto) {});
        auto reader = std::make_unique<TReader>(readerDestroyed);
        auto* raw = reader.get();
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetStream(*processor, std::move(reader), true);
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetLifetime(*processor,
                                                                     std::make_shared<TLease>(leaseDestroyed, readerDestroyed));
        TMessage message;
        processor->Read(&message, [&](NYdbGrpc::TGrpcStatus&& status) {
            UNIT_ASSERT_VALUES_EQUAL(status.GRpcStatusCode, static_cast<int>(grpc::StatusCode::RESOURCE_EXHAUSTED));
            UNIT_ASSERT(!leaseDestroyed);
            ++replies;
            processor.Reset();
        });
        NYdbGrpc::TStreamRequestReadProcessorTestAccess::SetDecodeError(*processor);
        raw->CompleteRead(false);
        UNIT_ASSERT_VALUES_EQUAL(raw->FinishCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(replies, 0);
        UNIT_ASSERT(!leaseDestroyed);
        raw->CompleteFinish(); // Transport reports CANCELLED, not the decoder error.
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        UNIT_ASSERT(readerDestroyed);
        UNIT_ASSERT(leaseDestroyed);
    }

    Y_UNIT_TEST(ConvertingSettingsPreserveControlAndLifetime) {
        struct TFirst: NYdb::TRequestSettings<TFirst> {};
        struct TSecond: NYdb::TRequestSettings<TSecond> {
            explicit TSecond(const TFirst& first)
                : TRequestSettings(first)
            {
            }
        };
        TFirst first;
        first.RequestControl(std::make_shared<NYdb::TRequestControl>()).RequestLifetime(std::make_shared<int>(1));
        const TSecond second(first);
        UNIT_ASSERT(first.RequestControl_ == second.RequestControl_);
        UNIT_ASSERT(first.RequestLifetime_ == second.RequestLifetime_);
    }
} // Y_UNIT_TEST_SUITE(RequestControlTests)
