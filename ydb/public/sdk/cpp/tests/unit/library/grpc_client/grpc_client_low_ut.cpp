#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <grpcpp/alarm.h>
#include <grpcpp/generic/generic_stub.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using namespace NYdbGrpc;

class TTestStub {
public:
    std::shared_ptr<grpc::ChannelInterface> ChannelInterface;
    TTestStub(std::shared_ptr<grpc::ChannelInterface> channelInterface)
        : ChannelInterface(channelInterface)
    {}
};

Y_UNIT_TEST_SUITE(ChannelPoolTests) {
    Y_UNIT_TEST(UnusedStubsHoldersDeletion) {
        TGRpcClientConfig clientConfig("invalid_host:invalid_port");
        TTcpKeepAliveSettings tcpKeepAliveSettings =
        {
            true,
            30, // NYdb::TCP_KEEPALIVE_IDLE, unused in UT, but is necessary in constructor
            5, // NYdb::TCP_KEEPALIVE_COUNT, unused in UT, but is necessary in constructor
            10 // NYdb::TCP_KEEPALIVE_INTERVAL, unused in UT, but is necessary in constructor
        };
        auto channelPool = TChannelPool(tcpKeepAliveSettings, TDuration::MilliSeconds(250), true);
        std::vector<std::weak_ptr<grpc::ChannelInterface>> ChannelInterfacesWeak;

        {
            std::vector<std::shared_ptr<TTestStub>> stubsHoldersShared;
            auto storeStubsHolders = [&](TStubsHolder& stubsHolder) {
                stubsHoldersShared.emplace_back(stubsHolder.GetOrCreateStub<TTestStub>());
                ChannelInterfacesWeak.emplace_back((*stubsHoldersShared.rbegin())->ChannelInterface);
                return;
            };
            for (int i = 0; i < 10; ++i) {
                channelPool.GetStubsHolderLocked(
                    ToString(i),
                    clientConfig,
                    storeStubsHolders
                );
            }
        }

        auto now = Now();
        while (Now() < now + TDuration::MilliSeconds(500)){
            Sleep(TDuration::MilliSeconds(100));
        }

        channelPool.DeleteExpiredStubsHolders();

        bool allDeleted = true;
        for (auto i = ChannelInterfacesWeak.begin(); i != ChannelInterfacesWeak.end(); ++i) {
            allDeleted = allDeleted && i->expired();
        }

        // assertion is made for channel interfaces instead of stubs, because after stub deletion
        // TStubsHolder has the only shared_ptr for channel interface.
        UNIT_ASSERT_C(allDeleted, "expired stubsHolders were not deleted after timeout");

    }
} // ChannelPoolTests ut suite

namespace {

class TAlarmProbe final : public TThrRefBase {
public:
    std::future<bool> Start(grpc::CompletionQueue* cq) {
        auto future = Completed_.get_future();
        Alarm_.Set(cq, NYdb::TDeadline::Now(), OnAlarmTag_.Prepare());
        return future;
    }

private:
    void OnAlarm(bool ok) {
        Completed_.set_value(ok);
    }

    grpc::Alarm Alarm_;
    std::promise<bool> Completed_;
    TQueueClientFixedEvent<TAlarmProbe> OnAlarmTag_ = {this, &TAlarmProbe::OnAlarm};
};

class TGenericTestService {
public:
    class Stub {
    public:
        explicit Stub(std::shared_ptr<grpc::ChannelInterface> channel)
            : Stub_(std::move(channel))
        {
        }

        std::unique_ptr<grpc::ClientAsyncResponseReader<grpc::ByteBuffer>> AsyncCall(
            grpc::ClientContext* context,
            const grpc::ByteBuffer& request,
            grpc::CompletionQueue* cq)
        {
            auto reader = Stub_.PrepareUnaryCall(context, "/sdk.runtime.test/Call", request, cq);
            reader->StartCall();
            return reader;
        }

    private:
        grpc::GenericStub Stub_;
    };

    static std::unique_ptr<Stub> NewStub(std::shared_ptr<grpc::ChannelInterface> channel) {
        return std::make_unique<Stub>(std::move(channel));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SharedNetworkTests) {
    Y_UNIT_TEST(ContextAndCompletionQueueSurviveLastHandle) {
        auto client = std::make_unique<TGRpcClientLow>(1);
        auto context = client->CreateContext();
        auto* cq = client->CompletionQueue();
        auto released = std::async(std::launch::async, [client = std::move(client)]() mutable {
            client.reset();
        });
        Y_SCOPE_EXIT(&context, &released) {
            context.reset();
            released.wait();
        };
        UNIT_ASSERT(released.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        UNIT_ASSERT(!context->IsCancelled());
        UNIT_ASSERT_VALUES_EQUAL(context->CreateContext()->CompletionQueue(), cq);

        TGRpcClientLow nextClient(4, true);
        UNIT_ASSERT_VALUES_EQUAL(nextClient.CompletionQueue(), cq);
        auto probe = MakeIntrusive<TAlarmProbe>();
        auto completed = probe->Start(cq);
        UNIT_ASSERT(completed.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        UNIT_ASSERT(completed.get());
    }

    Y_UNIT_TEST(StopAndWaitIdleAreNoops) {
        TGRpcClientLow client(1);
        auto context = client.CreateContext();
        client.Stop(false);
        auto stopped = std::async(std::launch::async, [&] {
            client.Stop(true);
            client.WaitIdle();
        });
        Y_SCOPE_EXIT(&context, &stopped) {
            context.reset();
            stopped.wait();
        };
        UNIT_ASSERT(stopped.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        UNIT_ASSERT(!client.IsStopping());
        UNIT_ASSERT(!context->IsCancelled());
        UNIT_ASSERT(client.CreateContext());
    }

    Y_UNIT_TEST(ExplicitCancellationPropagatesExactlyOnce) {
        TGRpcClientLow client(1);
        auto root = client.CreateContext();
        auto child = root->CreateContext();
        auto grandchild = child->CreateContext();
        auto independent = client.CreateContext();
        int callbacks = 0;
        root->SubscribeCancel([&] { ++callbacks; });
        child->SubscribeCancel([&] {
            ++callbacks;
            UNIT_ASSERT(root->CreateContext()->IsCancelled());
        });
        grandchild->SubscribeCancel([&] { ++callbacks; });

        UNIT_ASSERT(root->Cancel());
        UNIT_ASSERT(child->IsCancelled());
        UNIT_ASSERT(grandchild->IsCancelled());
        UNIT_ASSERT(!independent->IsCancelled());
        UNIT_ASSERT_VALUES_EQUAL(callbacks, 3);
        UNIT_ASSERT(!root->Cancel());
        UNIT_ASSERT(!child->Cancel());
        UNIT_ASSERT(!grandchild->Cancel());
        UNIT_ASSERT_VALUES_EQUAL(callbacks, 3);
        grandchild->SubscribeCancel([&] { ++callbacks; });
        UNIT_ASSERT_VALUES_EQUAL(callbacks, 4);
    }

    Y_UNIT_TEST(ChildCreationRacingWithCancellation) {
        TGRpcClientLow client(1);
        for (int iteration = 0; iteration < 64; ++iteration) {
            auto root = client.CreateContext();
            IQueueClientContextPtr child;
            std::atomic<int> callbacks = 0;
            std::promise<void> start;
            auto ready = start.get_future().share();
            std::thread creator([&] {
                ready.wait();
                child = root->CreateContext();
                child->SubscribeCancel([&] { ++callbacks; });
            });
            std::thread canceller([&] {
                ready.wait();
                root->Cancel();
            });
            start.set_value();
            creator.join();
            canceller.join();
            UNIT_ASSERT(child->IsCancelled());
            UNIT_ASSERT_VALUES_EQUAL(callbacks.load(), 1);
            UNIT_ASSERT(!child->Cancel());
        }
    }

    Y_UNIT_TEST(ServiceConnectionRetainsStableDefaultProvider) {
        std::unique_ptr<TServiceConnection<TGenericTestService>> connection;
        {
            TGRpcClientLow client(1);
            connection = client.CreateGRpcServiceConnection<TGenericTestService>(
                TGRpcClientConfig("127.0.0.1:1"));
        }

        auto promise = std::make_shared<std::promise<TGrpcStatus>>();
        auto completed = promise->get_future();
        TCallMeta meta;
        meta.Timeout = NYdb::TDeadline::AfterDuration(TDuration::Seconds(1));
        connection->DoRequest<grpc::ByteBuffer, grpc::ByteBuffer>(
            grpc::ByteBuffer(),
            [promise](TGrpcStatus&& status, grpc::ByteBuffer&&) {
                promise->set_value(std::move(status));
            },
            &TGenericTestService::Stub::AsyncCall,
            meta);
        UNIT_ASSERT(completed.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        UNIT_ASSERT(!completed.get().InternalError);
    }
}
