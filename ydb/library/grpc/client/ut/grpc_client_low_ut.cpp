#include <ydb/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/unittest/registar.h>

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
        auto channelPool = TChannelPool(tcpKeepAliveSettings, TDuration::MilliSeconds(250));
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