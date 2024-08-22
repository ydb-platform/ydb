#include "viable_peer_registry.h"

#include "client.h"
#include "config.h"
#include "indexed_hash_map.h"

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <library/cpp/yt/small_containers/compact_set.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

using namespace NNet;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TViablePeerRegistry
    : public IViablePeerRegistry
{
public:
    TViablePeerRegistry(
        TViablePeerRegistryConfigPtr config,
        TCreateChannelCallback createChannel,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , CreateChannel_(std::move(createChannel))
        , Logger(logger)
    {
        {
            auto guard = WriterGuard(SpinLock_);
            InitPeersAvailablePromise();
        }
    }

    bool RegisterPeer(const TString& address) override
    {
        int priority = 0;
        if (Config_->PeerPriorityStrategy == EPeerPriorityStrategy::PreferLocal) {
            priority = (InferYPClusterFromHostName(address) == GetLocalYPCluster()) ? 0 : 1;
        }

        bool wasEmpty = false;
        bool peerAdded = false;
        {
            auto guard = WriterGuard(SpinLock_);
            wasEmpty = ActivePeerToPriority_.Size() == 0;
            peerAdded = RegisterPeerWithPriority(address, priority);
        }

        if (wasEmpty && peerAdded) {
            GetPeersAvailablePromise(/*resetStoredError*/ true).TrySet();
        }

        return peerAdded;
    }

    bool UnregisterPeer(const TString& address) override
    {
        auto guard = WriterGuard(SpinLock_);
        bool peerUnregistered = GuardedUnregisterPeer(address);

        if (peerUnregistered) {
            OnPeerUnregistered();
        }

        return peerUnregistered;
    }

    bool UnregisterChannel(const TString& address, const IChannelPtr& channel) override
    {
        auto guard = WriterGuard(SpinLock_);

        auto it = ActivePeerToPriority_.find(address);
        if (it == ActivePeerToPriority_.end()) {
            return false;
        }

        auto storedChannel = GetOrCrash(PriorityToActivePeers_, it->second).Get(address);

        if (storedChannel != channel) {
            return false;
        }

        YT_VERIFY(GuardedUnregisterPeer(address));
        OnPeerUnregistered();

        return true;
    }

    std::vector<IChannelPtr> GetActiveChannels() const override
    {
        auto guard = ReaderGuard(SpinLock_);

        std::vector<IChannelPtr> allActivePeers;
        allActivePeers.reserve(ActivePeerToPriority_.Size());
        for (const auto& [priority, activePeers] : PriorityToActivePeers_) {
            for (const auto& [address, channel] : activePeers) {
                allActivePeers.push_back(channel);
            }
        }
        return allActivePeers;
    }

    void Clear() override
    {
        auto guard = WriterGuard(SpinLock_);

        BacklogPeers_.Clear();
        HashToActiveChannel_.clear();
        ActivePeerToPriority_.Clear();
        PriorityToActivePeers_.clear();

        InitPeersAvailablePromise();
    }

    std::optional<TString> MaybeRotateRandomPeer() override
    {
        auto guard = WriterGuard(SpinLock_);

        if (!BacklogPeerToPriority_.empty() && ActivePeerToPriority_.Size() > 0) {
            auto lastActivePriority = PriorityToActivePeers_.rbegin()->first;
            auto firstBacklogPriority = PriorityToBacklogPeers_.begin()->first;

            YT_LOG_DEBUG(
                "Trying to rotate random active peer (LastActivePriority: %v, FirstBacklogPriority: %v)",
                lastActivePriority,
                firstBacklogPriority);

            if (lastActivePriority < firstBacklogPriority) {
                return {};
            }

            // Invariant lastActivePriority <= firstBacklogPriority should hold.
            YT_VERIFY(lastActivePriority == firstBacklogPriority);

            const auto& activePeers = PriorityToActivePeers_.rbegin()->second;
            auto addressToEvict = activePeers.GetRandomElement().first;

            YT_LOG_DEBUG("Moving random viable peer to backlog (Address: %v)", addressToEvict);
            // This call will automatically activate a random peer from the backlog.
            GuardedUnregisterPeer(addressToEvict);
            // The rotated peer will end up in the backlog after this call.
            RegisterPeerWithPriority(addressToEvict, lastActivePriority);

            return addressToEvict;
        }
        return {};
    }

    IChannelPtr PickStickyChannel(const IClientRequestPtr& request) const override
    {
        auto guard = ReaderGuard(SpinLock_);

        if (BacklogPeers_.Size() > 0) {
            YT_LOG_WARNING(
                "Sticky channels are used with non-empty peer backlog, random peer rotations might hurt stickiness (MaxPeerCount: %v, ViablePeerCount: %v, BacklogPeerCount: %v)",
                Config_->MaxPeerCount,
                ActivePeerToPriority_.Size(),
                BacklogPeers_.Size());
        }

        const auto& balancingExt = request->Header().GetExtension(NProto::TBalancingExt::balancing_ext);
        auto hash = balancingExt.has_balancing_hint()
            ? balancingExt.balancing_hint()
            : request->GetHash();
        auto randomNumber = balancingExt.enable_client_stickiness() ? ClientStickinessRandomNumber_ : RandomNumber<size_t>();
        int stickyGroupSize = balancingExt.sticky_group_size();
        auto randomIndex = randomNumber % stickyGroupSize;

        if (ActivePeerToPriority_.Size() == 0) {
            return nullptr;
        }

        auto it = HashToActiveChannel_.lower_bound(std::pair(hash, TString()));
        auto rebaseIt = [&] {
            if (it == HashToActiveChannel_.end()) {
                it = HashToActiveChannel_.begin();
            }
        };

        TCompactSet<TStringBuf, 16> seenAddresses;
        auto currentRandomIndex = randomIndex % ActivePeerToPriority_.Size();
        while (true) {
            rebaseIt();
            const auto& address = it->first.second;
            if (seenAddresses.count(address) == 0) {
                if (currentRandomIndex == 0) {
                    break;
                }
                seenAddresses.insert(address);
                --currentRandomIndex;
            } else {
                ++it;
            }
        }

        YT_LOG_DEBUG(
            "Sticky peer selected (RequestId: %v, RequestHash: %x, RandomIndex: %v/%v, Address: %v)",
            request->GetRequestId(),
            hash,
            randomIndex,
            stickyGroupSize,
            it->first.second);

        return it->second;
    }

    // We only use this method for small counts, so this approach should work fine.
    static THashSet<int> GetRandomIndexes(int max, int count = 1)
    {
        THashSet<int> result;
        count = std::min(count, max);
        result.reserve(count);
        while (std::ssize(result) < count) {
            result.insert(static_cast<int>(RandomNumber<unsigned int>(max)));
        }

        return result;
    }

    std::vector<std::pair<TString, IChannelPtr>> PickRandomPeers(int peerCount = 1) const
    {
        VERIFY_READER_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(0 < peerCount && peerCount <= ActivePeerToPriority_.Size());

        int minPeersToPickFrom = std::max(Config_->MinPeerCountForPriorityAwareness, peerCount);

        std::vector<std::pair<TString, IChannelPtr>> peers;

        const auto& smallestPriorityPool = PriorityToActivePeers_.begin()->second;

        if (minPeersToPickFrom <= smallestPriorityPool.Size()) {
            for (const auto& index : GetRandomIndexes(smallestPriorityPool.Size(), peerCount)) {
                peers.push_back(smallestPriorityPool[index]);
            }
        } else {
            for (const auto& index : GetRandomIndexes(ActivePeerToPriority_.Size(), peerCount)) {
                const auto& [address, priority] = ActivePeerToPriority_[index];
                peers.emplace_back(
                    address,
                    GetOrCrash(PriorityToActivePeers_, priority).Get(address));
            }
        }

        return peers;
    }

    IChannelPtr PickChannelFromTwoRandom(const IClientRequestPtr& request) const
    {
        auto peers = PickRandomPeers(/*peerCount*/ 2);
        const auto& channelOne = peers.front();
        const auto& channelTwo = peers.back();

        auto getLoad = [] (const auto& channel) {
            return channel.second->GetInflightRequestCount();
        };

        const auto& theWinner = getLoad(channelOne) < getLoad(channelTwo) ? channelOne : channelTwo;

        YT_LOG_DEBUG(
            "Selected a peer via the power of two choices strategy (RequestId: %v, Peer1: %v, Peer2: %v, Winner: %v)",
            request ? request->GetRequestId() : TRequestId(),
            channelOne.first,
            channelTwo.first,
            theWinner.first);

        return theWinner.second;
    }

    IChannelPtr PickRandomChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions) const override
    {
        auto guard = ReaderGuard(SpinLock_);

        if (ActivePeerToPriority_.Size() == 0) {
            return nullptr;
        }

        IChannelPtr channel;
        if (hedgingOptions && hedgingOptions->HedgingManager && ActivePeerToPriority_.Size() >= 2) {
            auto peers = PickRandomPeers(/*peerCount*/ 2);
            const auto& primaryPeer = peers[0];
            const auto& backupPeer = peers[1];
            channel = CreateHedgingChannel(
                primaryPeer.second,
                backupPeer.second,
                *hedgingOptions);

            YT_LOG_DEBUG(
                "Random peers selected (RequestId: %v, PrimaryAddress: %v, BackupAddress: %v)",
                request ? request->GetRequestId() : TRequestId(),
                primaryPeer.first,
                backupPeer.first);
        } else if (Config_->EnablePowerOfTwoChoicesStrategy && ActivePeerToPriority_.Size() >= 2) {
            return PickChannelFromTwoRandom(request);
        } else {
            auto peer = PickRandomPeers()[0];
            channel = peer.second;

            YT_LOG_DEBUG(
                "Random peer selected (RequestId: %v, Address: %v)",
                request ? request->GetRequestId() : TRequestId(),
                peer.first);
        }

        return channel;
    }

    IChannelPtr GetChannel(const TString& address) const override
    {
        auto guard = ReaderGuard(SpinLock_);

        if (auto it = ActivePeerToPriority_.find(address); it != ActivePeerToPriority_.end()) {
            return GetOrCrash(PriorityToActivePeers_, it->second).Get(address);
        }
        return nullptr;
    }

    TFuture<void> GetPeersAvailable() const override
    {
        auto guard = ReaderGuard(SpinLock_);

        return PeersAvailablePromise_.ToFuture().ToUncancelable();
    }

    void SetError(const TError& error) override
    {
        GetPeersAvailablePromise().TrySet(error);
    }

private:
    const TViablePeerRegistryConfigPtr Config_;
    const TCallback<IChannelPtr(TString address)> CreateChannel_;
    const NLogging::TLogger Logger;

    const size_t ClientStickinessRandomNumber_ = RandomNumber<size_t>();


    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    // Information for active peers with created channels.
    std::map<int, TIndexedHashMap<TString, IChannelPtr>> PriorityToActivePeers_;
    TIndexedHashMap<TString, int> ActivePeerToPriority_;
    // A consistent-hashing storage for serving sticky requests.
    std::map<std::pair<size_t, TString>, IChannelPtr> HashToActiveChannel_;

    // Information for non-active peers which go over the max peer count limit.
    TIndexedHashMap<TString, int> BacklogPeers_;

    THashMap<TString, int> BacklogPeerToPriority_;
    std::map<int, TIndexedHashMap<TString, std::monostate>> PriorityToBacklogPeers_;

    TPromise<void> PeersAvailablePromise_;

    void InitPeersAvailablePromise()
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        YT_LOG_DEBUG("Awaiting peer availability");
        PeersAvailablePromise_ = NewPromise<void>();

        PeersAvailablePromise_.ToFuture().Subscribe(BIND([Logger = Logger] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Peers are available");
            } else {
                YT_LOG_DEBUG(error, "Error while awaiting peers");
            }
        }));
    }

    void OnPeerUnregistered()
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        if (ActivePeerToPriority_.Size() == 0 && PeersAvailablePromise_.IsSet()) {
            InitPeersAvailablePromise();
        }
    }

    TPromise<void> GetPeersAvailablePromise(bool resetStoredError = false)
    {
        auto guard = WriterGuard(SpinLock_);

        if (resetStoredError && PeersAvailablePromise_.IsSet() && !PeersAvailablePromise_.Get().IsOK()) {
            InitPeersAvailablePromise();
        }

        return PeersAvailablePromise_;
    }

    //! Returns true if a new peer was successfully registered and false if it already existed.
    //! Trying to call this method for a currently viable address with a different priority than stored leads to failure.
    bool RegisterPeerWithPriority(const TString& address, int priority)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        // Check for an existing active peer for this address.
        if (auto it = ActivePeerToPriority_.find(address); it != ActivePeerToPriority_.end()) {
            // Peers should have a fixed priority.
            YT_VERIFY(it->second == priority);
            return false;
        } else {
            // Peer is new, we need to check that we won't be adding more than MaxPeerCount active peers.
            if (ActivePeerToPriority_.Size() >= Config_->MaxPeerCount) {
                // Check for an existing backlog entry for this peer.
                if (auto backlogPeerIt = BacklogPeerToPriority_.find(address); backlogPeerIt != BacklogPeerToPriority_.end()) {
                    // Peers should have a fixed priority.
                    YT_VERIFY(backlogPeerIt->second == priority);
                    return false;
                } else {
                    // MaxPeerCount is required to be positive.
                    YT_VERIFY(!PriorityToActivePeers_.empty());
                    auto lastActivePeerPriority = PriorityToActivePeers_.rbegin()->first;
                    YT_LOG_DEBUG(
                        "Comparing priorities with active peers (LargestActivePeerPriority: %v, CurrentPeerPriority: %v)",
                        lastActivePeerPriority,
                        priority);

                    if (priority < lastActivePeerPriority) {
                        // If an active peer with lower priority than the one being added exists,
                        // we move it to the backlog.

                        auto activePeerAddressToEvict = PriorityToActivePeers_.rbegin()->second.GetRandomElement().first;
                        EraseActivePeer(activePeerAddressToEvict);
                        AddBacklogPeer(activePeerAddressToEvict, lastActivePeerPriority);
                        YT_LOG_DEBUG(
                            "Active peer evicted to backlog (Address: %v, Priority: %v, ReplacingAddress: %v)",
                            activePeerAddressToEvict,
                            lastActivePeerPriority,
                            address);
                        // We don't return here, since we still need to add our actual peer to the set of active peers.
                    } else {
                        AddBacklogPeer(address, priority);
                        YT_LOG_DEBUG(
                            "Viable peer added to backlog (Address: %v, Priority: %v)",
                            address,
                            priority);
                        return true;
                    }
                }
            }
        }

        AddActivePeer(address, priority);

        YT_LOG_DEBUG(
            "Activated viable peer (Address: %v, Priority: %v, ActivePeerCount: %v, BacklogPeerCount: %v, MaxPeerCount: %v)",
            address,
            priority,
            ActivePeerToPriority_.Size(),
            BacklogPeerToPriority_.size(),
            Config_->MaxPeerCount);

        return true;
    }

    template <class F>
    void GeneratePeerHashes(const TString& address, F f)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        TRandomGenerator generator(ComputeHash(address));
        for (int index = 0; index < Config_->HashesPerPeer; ++index) {
            f(generator.Generate<size_t>());
        }
    }

    bool GuardedUnregisterPeer(const TString& address)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        // Check if the peer is in the backlog and erase it if so.
        if (EraseBacklogPeer(address)) {
            YT_LOG_DEBUG(
                "Unregistered backlog peer (Address: %v, ActivePeerCount: %v, BacklogPeerCount: %v, MaxPeerCount: %v)",
                address,
                ActivePeerToPriority_.Size(),
                BacklogPeerToPriority_.size(),
                Config_->MaxPeerCount);
            return true;
        }

        // Check if the peer is active and erase it if so.
        if (EraseActivePeer(address)) {
            YT_LOG_DEBUG(
                "Unregistered active peer (Address: %v, ActivePeerCount: %v, BacklogPeerCount: %v, MaxPeerCount: %v)",
                address,
                ActivePeerToPriority_.Size(),
                BacklogPeerToPriority_.size(),
                Config_->MaxPeerCount);
            ActivateBacklogPeers();
            return true;
        }

        return false;
    }

    void ActivateBacklogPeers()
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        while (!BacklogPeerToPriority_.empty() && ActivePeerToPriority_.Size() < Config_->MaxPeerCount) {
            auto& [priority, backlogPeers] = *PriorityToBacklogPeers_.begin();
            auto randomBacklogPeer = backlogPeers.GetRandomElement();

            // Peer will definitely be activated, since the number of active peers is less than MaxPeerCount.
            RegisterPeerWithPriority(randomBacklogPeer.first, priority);
            YT_LOG_DEBUG("Activated peer from backlog (Address: %v, Priority: %v)", randomBacklogPeer.first, priority);

            // Until this moment the newly activated peer is still present in the backlog.
            EraseBacklogPeer(randomBacklogPeer.first);
        }
    }

    void AddActivePeer(const TString& address, int priority)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        ActivePeerToPriority_.Set(address, priority);

        auto channel = CreateChannel_(address);

        // Save the created channel for the given address for sticky requests.
        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToActiveChannel_[std::pair(hash, address)] = channel;
        });

        // Save the channel for the given address at its priority.
        PriorityToActivePeers_[priority].Set(address, channel);
    }

    void AddBacklogPeer(const TString& address, int priority)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        BacklogPeerToPriority_[address] = priority;
        PriorityToBacklogPeers_[priority].Set(address, {});
    }

    bool EraseActivePeer(const TString& address)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto activePeerIt = ActivePeerToPriority_.find(address);

        if (activePeerIt == ActivePeerToPriority_.end()) {
            return false;
        }

        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToActiveChannel_.erase(std::pair(hash, address));
        });

        auto activePeersForPriorityIt = PriorityToActivePeers_.find(activePeerIt->second);
        YT_VERIFY(activePeersForPriorityIt != PriorityToActivePeers_.end());
        activePeersForPriorityIt->second.Erase(address);
        if (activePeersForPriorityIt->second.Size() == 0) {
            PriorityToActivePeers_.erase(activePeersForPriorityIt);
        }

        ActivePeerToPriority_.Erase(address);

        return true;
    }

    bool EraseBacklogPeer(const TString& address)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto backlogPeerIt = BacklogPeerToPriority_.find(address);

        if (backlogPeerIt == BacklogPeerToPriority_.end()) {
            return false;
        }

        auto backlogPeersForPriorityIt = PriorityToBacklogPeers_.find(backlogPeerIt->second);
        YT_VERIFY(backlogPeersForPriorityIt != PriorityToBacklogPeers_.end());
        backlogPeersForPriorityIt->second.Erase(address);
        if (backlogPeersForPriorityIt->second.Size() == 0) {
            PriorityToBacklogPeers_.erase(backlogPeersForPriorityIt);
        }

        BacklogPeerToPriority_.erase(backlogPeerIt);

        return true;
    }
};

IViablePeerRegistryPtr CreateViablePeerRegistry(
    TViablePeerRegistryConfigPtr config,
    TCreateChannelCallback createChannel,
    const NLogging::TLogger& logger)
{
    return New<TViablePeerRegistry>(std::move(config), std::move(createChannel), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
