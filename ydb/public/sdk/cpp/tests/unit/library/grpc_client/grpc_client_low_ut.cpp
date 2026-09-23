#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdbGrpc;

class TTestStub {
public:
    std::shared_ptr<grpc::ChannelInterface> ChannelInterface;
    TTestStub(std::shared_ptr<grpc::ChannelInterface> channelInterface)
        : ChannelInterface(channelInterface)
    {}
};

namespace NYdbGrpc::inline Dev {

// The processor already grants TServiceConnection access to its queue and completion handler.
template <>
class TServiceConnection<TTestStub> {
public:
    static void CheckDroppedWriteLifetime(bool finishOk) {
        using TProcessor = TStreamRequestReadWriteProcessor<TTestStub, grpc::ByteBuffer, grpc::ByteBuffer>;
        size_t destroyed = 0;
        size_t callbacks = 0;
        char data = 0;
        auto processor = MakeIntrusive<TProcessor>([](TGrpcStatus&&, TProcessor::TBase::TPtr) {});
        for (size_t i = 0; i != 3; ++i) {
            grpc::Slice slice(&data, sizeof(data), [](void* counter) {
                ++*static_cast<size_t*>(counter);
            }, &destroyed);
            grpc::ByteBuffer request(&slice, 1);
            auto& item = processor->WriteQueue.emplace_back();
            item.Request.Swap(&request);
            item.Callback = [&, expected = i + 1](TGrpcStatus&& status) {
                UNIT_ASSERT(!status.Ok());
                UNIT_ASSERT_VALUES_EQUAL(destroyed, expected);
                UNIT_ASSERT_VALUES_EQUAL(++callbacks, expected);
            };
        }
        UNIT_ASSERT_VALUES_EQUAL(destroyed, 0);
        processor->OnFinished(finishOk);
        UNIT_ASSERT_VALUES_EQUAL(destroyed, 3);
        UNIT_ASSERT_VALUES_EQUAL(callbacks, 3);
    }
};

} // namespace NYdbGrpc::inline Dev

Y_UNIT_TEST_SUITE(StreamWriteTests) {
    Y_UNIT_TEST(DroppedRequestsAreReleasedBeforeCallbacks) {
        TServiceConnection<TTestStub>::CheckDroppedWriteLifetime(false);
        TServiceConnection<TTestStub>::CheckDroppedWriteLifetime(true);
    }
}

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
