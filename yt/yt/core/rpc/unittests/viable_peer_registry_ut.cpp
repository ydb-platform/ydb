#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/viable_peer_registry.h>
#include <yt/yt/core/rpc/indexed_hash_map.h>

namespace NYT::NRpc {
namespace {

using namespace NConcurrency;
using namespace NBus;

using testing::Pair;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;
using testing::AllOf;
using testing::IsSubsetOf;
using testing::Not;
using testing::Contains;
using testing::SizeIs;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger{"ViablePeerRegistryUnitTest"};

////////////////////////////////////////////////////////////////////////////////

TEST(TIndexedHashMapTest, Simple)
{
    TIndexedHashMap<TString, int> test;

    EXPECT_EQ(test.Size(), 0);

    EXPECT_TRUE(test.Set("a", 1));
    EXPECT_FALSE(test.Set("a", 2));
    EXPECT_EQ(test.Get("a"), 2);
    EXPECT_THAT(test[0], Pair("a", 2));
    EXPECT_EQ(test.Size(), 1);

    EXPECT_TRUE(test.Set("b", 3));
    EXPECT_THAT(test, UnorderedElementsAre(
        Pair("a", 2),
        Pair("b", 3)));
    EXPECT_EQ(test.Size(), 2);

    EXPECT_TRUE(test.Set("c", 42));
    EXPECT_EQ(test.Size(), 3);

    TIndexedHashMap<TString, int>::TUnderlyingStorage data;
    for (int i = 0; i < test.Size(); ++i) {
        data.push_back(test[i]);
    }
    EXPECT_THAT(test, UnorderedElementsAreArray(data));

    EXPECT_TRUE(test.find("d") == test.end());
    EXPECT_FALSE(test.find("a") == test.end());

    EXPECT_TRUE(test.Set("e", -1));
    EXPECT_TRUE(test.Set("f", 123));

    test.Erase(static_cast<int>(test.find("e") - test.begin()));

    EXPECT_FALSE(test.Erase("d"));
    EXPECT_TRUE(test.Erase("a"));
    EXPECT_EQ(test.Size(), 3);
    EXPECT_THAT(test, UnorderedElementsAre(
        Pair("b", 3),
        Pair("c", 42),
        Pair("f", 123)));

    test.Clear();
    EXPECT_EQ(test.Size(), 0);
    EXPECT_THAT(test, UnorderedElementsAre());
}

class TFakeChannel
    : public IChannel
{
public:
    TFakeChannel(TString address, THashSet<TString>* channelRegistry)
        : Address_(std::move(address))
        , ChannelRegistry_(channelRegistry)
    {
        if (ChannelRegistry_) {
            ChannelRegistry_->insert(Address_);
        }
    }

    const TString& GetEndpointDescription() const override
    {
        return Address_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        YT_UNIMPLEMENTED();
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr /*request*/,
        IClientResponseHandlerPtr /*responseHandler*/,
        const TSendOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

    ~TFakeChannel() override
    {
        if (ChannelRegistry_) {
            YT_VERIFY(ChannelRegistry_->erase(Address_));
        }
    }

    int GetInflightRequestCount() override
    {
        return 0;
    }

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override
    {
        return MemoryUsageTracker_;
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Terminated);
private:
    const IMemoryUsageTrackerPtr MemoryUsageTracker_ = GetNullMemoryUsageTracker();

    TString Address_;
    THashSet<TString>* ChannelRegistry_;
};

class TFakeChannelFactory
    : public IChannelFactory
{
public:
    IChannelPtr CreateChannel(const TString& address) override
    {
        return New<TFakeChannel>(address, &ChannelRegistry_);
    }

    const THashSet<TString>& GetChannelRegistry() const
    {
        return ChannelRegistry_;
    }

private:
    THashSet<TString> ChannelRegistry_;
};

IViablePeerRegistryPtr CreateTestRegistry(
    EPeerPriorityStrategy peerPriorityStrategy,
    const IChannelFactoryPtr& channelFactory,
    int maxPeerCount,
    std::optional<int> hashesPerPeer = {},
    std::optional<int> minPeerCountForPriorityAwareness = {})
{
    auto config = New<TViablePeerRegistryConfig>();
    config->MaxPeerCount = maxPeerCount;
    if (hashesPerPeer) {
        config->HashesPerPeer = *hashesPerPeer;
    }
    if (minPeerCountForPriorityAwareness) {
        config->MinPeerCountForPriorityAwareness = *minPeerCountForPriorityAwareness;
    }

    config->PeerPriorityStrategy = peerPriorityStrategy;

    return CreateViablePeerRegistry(
        config,
        BIND([=] (const TString& address) { return channelFactory->CreateChannel(address); }),
        Logger);
}

std::vector<TString> AddressesFromChannels(const std::vector<IChannelPtr>& channels)
{
    std::vector<TString> result;
    for (const auto& channel: channels) {
        result.push_back(channel->GetEndpointDescription());
    }
    return result;
}

class TParametrizedViablePeerRegistryTest
    : public testing::TestWithParam<EPeerPriorityStrategy>
{ };

TEST_P(TParametrizedViablePeerRegistryTest, Simple)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 3);

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));
    EXPECT_FALSE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d"));

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "b", "c"));
    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAreArray(AddressesFromChannels(viablePeerRegistry->GetActiveChannels())));

    viablePeerRegistry->UnregisterPeer("b");

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "c", "d"));
    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAreArray(AddressesFromChannels(viablePeerRegistry->GetActiveChannels())));

    // Peer "b" should end up in the backlog.
    viablePeerRegistry->RegisterPeer("b");
    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "c", "d"));
    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAreArray(AddressesFromChannels(viablePeerRegistry->GetActiveChannels())));

    EXPECT_TRUE(viablePeerRegistry->MaybeRotateRandomPeer());

    // Backlog contained a single peer "b", so it should be activated now.
    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        AllOf(SizeIs(3), IsSubsetOf({"a", "b", "c", "d"}), Contains("b")));
    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAreArray(AddressesFromChannels(viablePeerRegistry->GetActiveChannels())));
}

TEST_P(TParametrizedViablePeerRegistryTest, RotateOnlyActivePeer)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 1);

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));

    auto rotatedPeer = viablePeerRegistry->MaybeRotateRandomPeer();
    EXPECT_TRUE(rotatedPeer);
    EXPECT_EQ(*rotatedPeer, "a");
}

TEST_P(TParametrizedViablePeerRegistryTest, UnregisterFromBacklog)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 2);

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d"));

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "b"));

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("c"));

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "b"));

    auto rotatedPeer = viablePeerRegistry->MaybeRotateRandomPeer();
    EXPECT_TRUE(rotatedPeer);
    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        AllOf(SizeIs(2), IsSubsetOf({"a", "b", "d"}), Not(Contains(*rotatedPeer)), Contains("d")));

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer(*rotatedPeer));
    EXPECT_FALSE(viablePeerRegistry->MaybeRotateRandomPeer());

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("d"));
    EXPECT_THAT(channelFactory->GetChannelRegistry(), AllOf(SizeIs(1), IsSubsetOf({"a", "b"})));
}

TEST_P(TParametrizedViablePeerRegistryTest, Clear)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 2);

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d"));

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "b"));

    viablePeerRegistry->Clear();

    EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre());
}

class TFakeRequest
    : public TClientRequest
{
public:
    TFakeRequest()
        : TClientRequest(
            New<TFakeChannel>("fake", nullptr),
            TServiceDescriptor{"service"},
            TMethodDescriptor{"method"})
    { }

    TSharedRefArray SerializeHeaderless() const override
    {
        YT_UNIMPLEMENTED();
    }

    size_t GetHash() const override
    {
        return Hash_;
    }

private:
    size_t Hash_ = RandomNumber<size_t>();
};

IClientRequestPtr CreateRequest(bool enableStickiness = false)
{
    auto request = New<TFakeRequest>();
    auto balancingHeaderExt = request->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
    balancingHeaderExt->set_enable_stickiness(enableStickiness);
    return request;
}

TEST_P(TParametrizedViablePeerRegistryTest, GetChannelBasic)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 3);

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("e"));

    TString retrievedPeer;
    {
        auto channel = viablePeerRegistry->PickRandomChannel(CreateRequest(), /*hedgingOptions*/ {});
        retrievedPeer = channel->GetEndpointDescription();
        EXPECT_THAT(channelFactory->GetChannelRegistry(), UnorderedElementsAre("a", "b", "c"));
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(retrievedPeer));
    }

    viablePeerRegistry->UnregisterPeer(retrievedPeer);
    EXPECT_THAT(channelFactory->GetChannelRegistry(), AllOf(
        SizeIs(3),
        IsSubsetOf({"a", "b", "c", "d", "e"}),
        Not(Contains(retrievedPeer))));

    {
        auto channel = viablePeerRegistry->PickRandomChannel(CreateRequest(), /*hedgingOptions*/ {});;
        EXPECT_NE(channel->GetEndpointDescription(), retrievedPeer);
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(channel->GetEndpointDescription()));
    }
}

TEST_P(TParametrizedViablePeerRegistryTest, GetRandomChannel)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 100);

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(viablePeerRegistry->RegisterPeer(Format("address-%v", i)));
    }

    THashSet<TString> retrievedAddresses;

    auto req = CreateRequest();
    for (int iter = 0; iter < 100; ++iter) {
        auto channel = viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
        retrievedAddresses.insert(channel->GetEndpointDescription());
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(channel->GetEndpointDescription()));
    }

    // The probability of this failing should be 1e-200.
    EXPECT_GT(retrievedAddresses.size(), 1u);
}

TEST_P(TParametrizedViablePeerRegistryTest, GetStickyChannel)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 2000, 10);

    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(viablePeerRegistry->RegisterPeer(Format("address-%v", i)));
    }

    THashSet<TString> retrievedAddresses;

    auto req = CreateRequest(/*enableStickiness*/ true);
    for (int iter = 0; iter < 100; ++iter) {
        auto channel = viablePeerRegistry->PickStickyChannel(req);
        retrievedAddresses.insert(channel->GetEndpointDescription());
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(channel->GetEndpointDescription()));
    }

    EXPECT_EQ(retrievedAddresses.size(), 1u);

    THashMap<IClientRequestPtr, TString> requestToPeer;
    for (int iter = 0; iter < 1000; ++iter) {
        auto request = CreateRequest(/*enableStickiness*/ true);
        auto channel = viablePeerRegistry->PickStickyChannel(request);
        auto peer = channel->GetEndpointDescription();
        requestToPeer[request] = peer;
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(peer));
    }

    for (int i = 0; i < 11; ++i) {
        EXPECT_TRUE(viablePeerRegistry->RegisterPeer(Format("address-%v-2", i)));
    }

    int misses = 0;
    for (const auto& [request, peer] : requestToPeer) {
        auto channel = viablePeerRegistry->PickStickyChannel(request);
        if (channel->GetEndpointDescription() != peer) {
            ++misses;
        }
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(channel->GetEndpointDescription()));
    }

    EXPECT_LE(misses, 100);
}

TEST_P(TParametrizedViablePeerRegistryTest, PeersAvailablePromise)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(GetParam(), channelFactory, 1);

    auto peersAvailable1 = viablePeerRegistry->GetPeersAvailable();
    EXPECT_FALSE(peersAvailable1.IsSet());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());
    EXPECT_TRUE(peersAvailable1.IsSet());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("c"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_TRUE(viablePeerRegistry->MaybeRotateRandomPeer());
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("a"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("b"));
    EXPECT_FALSE(viablePeerRegistry->GetPeersAvailable().IsSet());

    EXPECT_FALSE(viablePeerRegistry->UnregisterPeer("e"));
    EXPECT_FALSE(viablePeerRegistry->GetPeersAvailable().IsSet());

    viablePeerRegistry->SetError(TError("error"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());
    EXPECT_FALSE(viablePeerRegistry->GetPeersAvailable().Get().IsOK());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("f"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().IsSet());
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().Get().IsOK());

    viablePeerRegistry->SetError(TError("another error"));
    EXPECT_TRUE(viablePeerRegistry->GetPeersAvailable().Get().IsOK());

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("f"));
    EXPECT_FALSE(viablePeerRegistry->GetPeersAvailable().IsSet());

    auto startTime = TInstant::Now();

    TDelayedExecutor::Submit(BIND([viablePeerRegistry] {
        viablePeerRegistry->RegisterPeer("i_am_available");
    }), TDuration::Seconds(5));

    auto channel = WaitFor(viablePeerRegistry->GetPeersAvailable()
        .Apply(BIND([viablePeerRegistry] {
            auto req = CreateRequest();
            return viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
        })))
        .ValueOrThrow();

    auto elapsedTime = TInstant::Now() - startTime;

    EXPECT_GE(elapsedTime, TDuration::Seconds(3));
    EXPECT_LE(elapsedTime, TDuration::Seconds(7));

    EXPECT_EQ(channel->GetEndpointDescription(), "i_am_available");
}

INSTANTIATE_TEST_SUITE_P(
    AllPriorityStrategies,
    TParametrizedViablePeerRegistryTest,
    testing::Values(EPeerPriorityStrategy::None, EPeerPriorityStrategy::PreferLocal));

TEST(TPreferLocalViablePeerRegistryTest, Simple)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(EPeerPriorityStrategy::PreferLocal, channelFactory, 3);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d.sas.yp-c.yandex.net"));

    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        UnorderedElementsAre("b.sas.yp-c.yandex.net", "c.sas.yp-c.yandex.net", "a.man.yp-c.yandex.net"));

    auto req = CreateRequest();
    for (int iter = 0; iter < 100; ++iter) {
        auto channel = viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
        EXPECT_EQ(channel->GetEndpointDescription(), "a.man.yp-c.yandex.net");
    }
}

TEST(TPreferLocalViablePeerRegistryTest, MinPeerCountForPriorityAwareness)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(
        EPeerPriorityStrategy::PreferLocal,
        channelFactory,
        /*maxPeerCount*/ 100,
        /*hashesPerPeer*/ {},
        /*minPeerCountForPriorityAwareness*/ 2);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("local.man.yp-c.yandex.net"));

    for (int i = 0; i < 99; ++i) {
        EXPECT_TRUE(viablePeerRegistry->RegisterPeer(Format("non-local-%v.sas.yp-c.yandex.net", i)));
    }

    THashSet<TString> retrievedAddresses;

    auto req = CreateRequest();
    for (int iter = 0; iter < 100; ++iter) {
        auto channel = viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
        retrievedAddresses.insert(channel->GetEndpointDescription());
        EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains(channel->GetEndpointDescription()));
    }

    // The probability of this failing should be 1e-200.
    EXPECT_GT(retrievedAddresses.size(), 1u);
}

TEST(TPreferLocalViablePeerRegistryTest, RegistrationEvictsLesserPeers)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(EPeerPriorityStrategy::PreferLocal, channelFactory, 3);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("e.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("f.man.yp-c.yandex.net"));

    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        UnorderedElementsAre("a.man.yp-c.yandex.net", "e.man.yp-c.yandex.net", "f.man.yp-c.yandex.net"));

    EXPECT_FALSE(viablePeerRegistry->MaybeRotateRandomPeer());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("g.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->MaybeRotateRandomPeer());
    EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains("g.man.yp-c.yandex.net"));
}

TEST(TPreferLocalViablePeerRegistryTest, PeerRotationRespectsPriority)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(EPeerPriorityStrategy::PreferLocal, channelFactory, 3);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("e.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("f.man.yp-c.yandex.net"));

    EXPECT_FALSE(viablePeerRegistry->MaybeRotateRandomPeer());

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("g.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->MaybeRotateRandomPeer());
    EXPECT_THAT(channelFactory->GetChannelRegistry(), Contains("g.man.yp-c.yandex.net"));
}

TEST(TPreferLocalViablePeerRegistryTest, FillFromBacklogRespectsPriority)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(EPeerPriorityStrategy::PreferLocal, channelFactory, 3);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("c.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("d.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("e.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("f.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("g.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("h.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("i.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("j.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("k.man.yp-c.yandex.net"));

    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        UnorderedElementsAre("a.man.yp-c.yandex.net", "e.man.yp-c.yandex.net", "f.man.yp-c.yandex.net"));

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("a.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("e.man.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("f.man.yp-c.yandex.net"));

    EXPECT_THAT(
        channelFactory->GetChannelRegistry(),
        UnorderedElementsAre("h.man.yp-c.yandex.net", "j.man.yp-c.yandex.net", "k.man.yp-c.yandex.net"));
}

TEST(TPreferLocalViablePeerRegistryTest, DoNotCrashIfNoLocalPeers)
{
    auto channelFactory = New<TFakeChannelFactory>();
    auto viablePeerRegistry = CreateTestRegistry(EPeerPriorityStrategy::PreferLocal, channelFactory, 3);

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("home.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("b.sas.yp-c.yandex.net"));
    EXPECT_TRUE(viablePeerRegistry->RegisterPeer("a.man.yp-c.yandex.net"));

    auto req = CreateRequest();
    auto localChannel = viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
    EXPECT_EQ(localChannel->GetEndpointDescription(), "a.man.yp-c.yandex.net");

    EXPECT_TRUE(viablePeerRegistry->UnregisterPeer("a.man.yp-c.yandex.net"));
    auto otherChannel = viablePeerRegistry->PickRandomChannel(req, /*hedgingOptions*/ {});
    EXPECT_EQ(otherChannel->GetEndpointDescription(), "b.sas.yp-c.yandex.net");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
