#include "dynamic_channel_pool.h"

#include "dispatcher.h"
#include "client.h"
#include "config.h"
#include "private.h"
#include "viable_peer_registry.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/utilex/random.h>

#include <library/cpp/yt/misc/variant.h>

#include <library/cpp/yt/small_containers/compact_set.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/random/shuffle.h>

#include <util/generic/algorithm.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NThreading;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPool::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDynamicChannelPoolConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TString endpointDescription,
        IAttributeDictionaryPtr endpointAttributes,
        std::string serviceName,
        TDiscoverRequestHook discoverRequestHook)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , EndpointDescription_(std::move(endpointDescription))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(*endpointAttributes)
                .Item("service").Value(serviceName)
            .EndMap()))
        , ServiceName_(std::move(serviceName))
        , DiscoverRequestHook_(std::move(discoverRequestHook))
        , Logger(RpcClientLogger.WithTag(
            "ChannelId: %v, Endpoint: %v, Service: %v",
            TGuid::Create(),
            EndpointDescription_,
            ServiceName_))
       , ViablePeerRegistry_(CreateViablePeerRegistry(
            Config_,
            BIND(&TImpl::CreateChannel, Unretained(this)),
            Logger))
       , RandomPeerRotationExecutor_(New<TPeriodicExecutor>(
           TDispatcher::Get()->GetLightInvoker(),
           BIND(&TDynamicChannelPool::TImpl::MaybeEvictRandomPeer, MakeWeak(this)),
           Config_->RandomPeerEvictionPeriod))
    {
        RandomPeerRotationExecutor_->Start();
    }

    TFuture<IChannelPtr> GetRandomChannel()
    {
        return GetChannel(
            /*request*/ nullptr,
            /*hedgingOptions*/ std::nullopt);
    }

    TFuture<IChannelPtr> GetChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions)
    {
        if (auto channel = PickViableChannel(request, hedgingOptions)) {
            return MakeFuture(channel);
        }

        auto sessionOrError = RunDiscoverySession();
        if (!sessionOrError.IsOK()) {
            return MakeFuture<IChannelPtr>(TError(sessionOrError));
        }

        const auto& session = sessionOrError.Value();

        // TODO(achulkov2): kill GetFinished.
        auto future = IsRequestSticky(request)
                      ? session->GetFinished()
                      : ViablePeerRegistry_->GetPeersAvailable();
        YT_LOG_DEBUG_IF(!future.IsSet(), "Channel requested, waiting on peers to become available");
        return future.Apply(BIND([this_ = MakeWeak(this), request, hedgingOptions] {
            if (auto strongThis = this_.Lock()) {
                auto channel = strongThis->PickViableChannel(request, hedgingOptions);
                if (!channel) {
                    // Not very likely but possible in theory.
                    THROW_ERROR strongThis->MakeNoAlivePeersError();
                }
                return channel;
            } else {
                THROW_ERROR_EXCEPTION("Cannot get channel, dynamic channel pool is being destroyed");
            }
        }));
    }

    void SetPeers(std::vector<TString> addresses)
    {
        SortUnique(addresses);
        Shuffle(addresses.begin(), addresses.end());
        THashSet<TString> addressSet(addresses.begin(), addresses.end());

        {
            auto guard = WriterGuard(SpinLock_);

            std::vector<TString> addressesToRemove;

            for (const auto& address : ActiveAddresses_) {
                if (!addressSet.contains(address)) {
                    addressesToRemove.push_back(address);
                }
            }

            for (const auto& address : BannedAddresses_) {
                if (!addressSet.contains(address)) {
                    addressesToRemove.push_back(address);
                }
            }

            for (const auto& address : addressesToRemove) {
                RemovePeer(address);
            }

            DoAddPeers(addresses);
        }

        PeersSetPromise_.TrySet();
    }

    void SetPeerDiscoveryError(const TError& error)
    {
        {
            auto guard = WriterGuard(SpinLock_);
            PeerDiscoveryError_ = error;
        }

        PeersSetPromise_.TrySet();
    }

    void Terminate(const TError& error)
    {
        // Holds a weak reference to this class and the callback has no meaningful side-effects,
        // so not waiting on this future is OK.
        YT_UNUSED_FUTURE(RandomPeerRotationExecutor_->Stop());

        std::vector<IChannelPtr> activeChannels;

        {
            auto guard = WriterGuard(SpinLock_);
            Terminated_ = true;
            TerminationError_ = error;
            activeChannels = ViablePeerRegistry_->GetActiveChannels();
            ViablePeerRegistry_->Clear();
        }

        for (auto& channel : activeChannels) {
            channel->Terminate(error);
        }
    }

private:
    class TDiscoverySession;
    using TDiscoverySessionPtr = TIntrusivePtr<TDiscoverySession>;

    class TPeerPoller;
    using TPeerPollerPtr = TIntrusivePtr<TPeerPoller>;

    const TDynamicChannelPoolConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const std::string ServiceName_;
    const TDiscoverRequestHook DiscoverRequestHook_;

    const NLogging::TLogger Logger;

    const TPromise<void> PeersSetPromise_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    bool Terminated_ = false;
    TDiscoverySessionPtr CurrentDiscoverySession_;
    TDelayedExecutorCookie RediscoveryCookie_;
    TError TerminationError_;
    TError PeerDiscoveryError_;

    THashSet<TString> ActiveAddresses_;
    THashSet<TString> BannedAddresses_;

    THashMap<TString, TPeerPollerPtr> AddressToPoller_;

    IViablePeerRegistryPtr ViablePeerRegistry_;

    const TPeriodicExecutorPtr RandomPeerRotationExecutor_;

    struct TTooManyConcurrentRequests { };
    struct TNoMorePeers { };

    using TPickPeerResult = std::variant<
        TString,
        TTooManyConcurrentRequests,
        TNoMorePeers>;

    class TDiscoverySession
        : public TRefCounted
    {
    public:
        explicit TDiscoverySession(TImpl* owner)
            : Owner_(owner)
            , Config_(owner->Config_)
            , Logger(owner->Logger)
        { }

        TFuture<void> GetFinished()
        {
            return FinishedPromise_;
        }

        void Run()
        {
            YT_LOG_DEBUG("Starting peer discovery");
            TDispatcher::Get()->GetLightInvoker()->Invoke(BIND(&TDiscoverySession::DoRun, MakeStrong(this)));
        }

        void OnPeerDiscovered(const TString& address)
        {
            AddViablePeer(address);
            Success_.store(true);
        }

    private:
        const TWeakPtr<TImpl> Owner_;
        const TDynamicChannelPoolConfigPtr Config_;
        const NLogging::TLogger Logger;

        const TPromise<void> FinishedPromise_ = NewPromise<void>();
        std::atomic<bool> Finished_ = false;
        std::atomic<bool> Success_ = false;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
        THashSet<TString> RequestedAddresses_;
        THashSet<TString> RequestingAddresses_;

        constexpr static int MaxDiscoveryErrorsToKeep = 100;
        std::deque<TError> DiscoveryErrors_;

        void DoRun()
        {
            auto deadline = TInstant::Now() + Config_->DiscoverySessionTimeout;

            while (true) {
                if (TInstant::Now() > deadline) {
                    OnFinished();
                    break;
                }

                auto mustBreak = false;
                auto pickResult = PickPeer();
                Visit(pickResult,
                    [&] (TTooManyConcurrentRequests) {
                        mustBreak = true;
                    },
                    [&] (TNoMorePeers) {
                        if (!HasOutstandingQueries()) {
                            OnFinished();
                        }
                        mustBreak = true;
                    },
                    [&] (const TString& address) {
                        QueryPeer(address);
                    });

                if (mustBreak) {
                    break;
                }
            }
        }

        void QueryPeer(const TString& address)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            YT_LOG_DEBUG("Querying peer (Address: %v)", address);

            auto channel = owner->ChannelFactory_->CreateChannel(address);
            auto proxy = owner->CreateGenericProxy(channel);
            proxy.SetDefaultTimeout(owner->Config_->DiscoverTimeout);

            auto req = proxy.Discover();
            if (owner->DiscoverRequestHook_) {
                owner->DiscoverRequestHook_.Run(req.Get());
            }

            // NB: Via prevents stack overflow due to QueryPeer -> OnResponse -> DoRun loop in
            // case when Invoke() is immediately set.
            req->Invoke().Subscribe(BIND(
                &TDiscoverySession::OnResponse,
                MakeStrong(this),
                address)
                .Via(TDispatcher::Get()->GetLightInvoker()));
        }

        void OnResponse(
            const TString& address,
            const TGenericProxy::TErrorOrRspDiscoverPtr& rspOrError)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            // COMPAT(babenko): drop this once all RPC proxies support unauthenticated Discover requests
            bool authError = rspOrError.GetCode() == NRpc::EErrorCode::AuthenticationError;
            YT_LOG_DEBUG_IF(authError, "Peer has reported authentication error on discovery (Address: %v)",
                address);
            if (rspOrError.IsOK() || authError) {
                // const auto& rsp = rspOrError.Value();
                // bool up = rsp->up();
                // auto suggestedAddresses = FromProto<std::vector<TString>>(rsp->suggested_addresses());
                bool up = authError ? true : rspOrError.Value()->up();
                auto suggestedAddresses = authError ? std::vector<TString>() : FromProto<std::vector<TString>>(rspOrError.Value()->suggested_addresses());

                if (!suggestedAddresses.empty()) {
                    YT_LOG_DEBUG("Peers suggested (SuggestorAddress: %v, SuggestedAddresses: %v)",
                        address,
                        suggestedAddresses);
                    owner->AddPeers(suggestedAddresses);
                }

                YT_LOG_DEBUG("Peer has reported its state (Address: %v, Up: %v)",
                    address,
                    up);

                if (up) {
                    OnPeerDiscovered(address);
                } else {
                    auto error = owner->MakePeerDownError(address);
                    BanPeer(address, error, owner->Config_->SoftBackoffTime);
                    InvalidatePeer(address);
                }
            } else {
                YT_LOG_DEBUG(rspOrError, "Peer discovery request failed (Address: %v)",
                    address);
                auto error = owner->MakePeerDiscoveryFailedError(address, rspOrError);
                BanPeer(address, error, owner->Config_->HardBackoffTime);
                InvalidatePeer(address);
            }

            OnPeerQueried(address);
            DoRun();
        }

        TPickPeerResult PickPeer()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return TNoMorePeers();
            }

            auto guard = Guard(SpinLock_);
            return owner->PickPeer(&RequestingAddresses_, &RequestedAddresses_);
        }

        void OnPeerQueried(const TString& address)
        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(RequestingAddresses_.erase(address) == 1);
        }

        bool HasOutstandingQueries()
        {
            auto guard = Guard(SpinLock_);
            return !RequestingAddresses_.empty();
        }

        void BanPeer(const TString& address, const TError& error, TDuration backoffTime)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            {
                auto guard = Guard(SpinLock_);
                YT_VERIFY(RequestedAddresses_.erase(address) == 1);

                DiscoveryErrors_.push_back(error);
                while (std::ssize(DiscoveryErrors_) > MaxDiscoveryErrorsToKeep) {
                    DiscoveryErrors_.pop_front();
                }
            }

            owner->BanPeer(address, backoffTime);
        }

        std::vector<TError> GetDiscoveryErrors()
        {
            auto guard = Guard(SpinLock_);
            return {DiscoveryErrors_.begin(), DiscoveryErrors_.end()};
        }

        void AddViablePeer(const TString& address)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            owner->AddViablePeer(address);
        }

        void InvalidatePeer(const TString& address)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            owner->InvalidatePeer(address);
        }

        void OnFinished()
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            if (Finished_.exchange(true)) {
                return;
            }

            if (Success_.load()) {
                FinishedPromise_.Set();
            } else {
                auto error = owner->MakeNoAlivePeersError()
                    << GetDiscoveryErrors();
                YT_LOG_DEBUG(error, "Error performing peer discovery");
                owner->ViablePeerRegistry_->SetError(error);
                FinishedPromise_.Set(error);
            }
        }
    };

    class TPeerPoller
        : public TRefCounted
    {
    public:
        TPeerPoller(TImpl* owner, TString peerAddress)
            : Owner_(owner)
            , Logger(owner->Logger.WithTag("Address: %v", peerAddress))
            , PeerAddress_(std::move(peerAddress))
        { }

        void Run()
        {
            YT_LOG_DEBUG("Starting peer poller");
            TDispatcher::Get()->GetLightInvoker()->Invoke(BIND(&TPeerPoller::DoRun, MakeStrong(this)));
        }

        void Stop()
        {
            YT_LOG_DEBUG("Stopping peer poller");
            Stopped_ = true;
        }

    private:
        const TWeakPtr<TImpl> Owner_;
        const NLogging::TLogger Logger;

        const TString PeerAddress_;

        std::atomic<bool> Stopped_ = false;

        TInstant LastRequestStart_ = TInstant::Zero();

        void DoRun()
        {
            {
                auto owner = Owner_.Lock();
                if (!owner) {
                    return;
                }

                auto delay = RandomDuration(owner->Config_->PeerPollingPeriodSplay);
                YT_LOG_DEBUG("Sleeping before peer polling start (Delay: %v)",
                    delay);
                TDelayedExecutor::WaitForDuration(delay);
            }

            DoPollPeer();
        }

        void DoPollPeer(TDuration lastPeerPollingPeriod = TDuration::Zero())
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            if (Stopped_) {
                return;
            }

            auto now = TInstant::Now();
            if (LastRequestStart_ + lastPeerPollingPeriod > now) {
                auto delay = LastRequestStart_ + lastPeerPollingPeriod - now;
                YT_LOG_DEBUG("Sleeping before peer polling (Delay: %v)",
                    delay);
                TDelayedExecutor::WaitForDuration(delay);
            }

            LastRequestStart_ = now;
            auto peerPollingPeriod = owner->Config_->PeerPollingPeriod + RandomDuration(owner->Config_->PeerPollingPeriodSplay);

            auto channel = owner->ChannelFactory_->CreateChannel(PeerAddress_);
            auto proxy = owner->CreateGenericProxy(channel);

            auto requestTimeout = peerPollingPeriod + owner->Config_->PeerPollingRequestTimeout;
            auto req = proxy.Discover();
            req->set_reply_delay(peerPollingPeriod.GetValue());
            req->SetTimeout(requestTimeout);
            if (owner->DiscoverRequestHook_) {
                owner->DiscoverRequestHook_.Run(req.Get());
            }

            YT_LOG_DEBUG("Polling peer (PollingPeriod: %v, RequestTimeout: %v)",
                peerPollingPeriod,
                requestTimeout);

            owner.Reset();

            req->Invoke()
                .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TGenericProxy::TErrorOrRspDiscoverPtr& rspOrError) {
                    auto owner = Owner_.Lock();
                    if (!owner) {
                        return;
                    }

                    if (rspOrError.IsOK()) {
                        auto isUp = rspOrError.Value()->up();
                        if (isUp) {
                            YT_LOG_DEBUG("Peer is up");
                            owner->UnbanPeer(PeerAddress_);
                            auto discoverySessionOrError = owner->RunDiscoverySession();
                            if (discoverySessionOrError.IsOK()) {
                                discoverySessionOrError.Value()->OnPeerDiscovered(PeerAddress_);
                            } else {
                                YT_LOG_DEBUG(discoverySessionOrError, "Failed to get discovery session");
                            }
                        } else {
                            YT_LOG_DEBUG("Peer is down");
                        }
                    } else {
                        YT_LOG_DEBUG(rspOrError, "Failed to poll peer");
                    }

                    DoPollPeer(peerPollingPeriod);
                }).Via(TDispatcher::Get()->GetLightInvoker()));
        }
    };

    IChannelPtr PickViableChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions)
    {
        return IsRequestSticky(request)
            ? ViablePeerRegistry_->PickStickyChannel(request)
            : ViablePeerRegistry_->PickRandomChannel(request, hedgingOptions);
    }

    TErrorOr<TDiscoverySessionPtr> RunDiscoverySession()
    {
        TDiscoverySessionPtr session;
        {
            auto guard = WriterGuard(SpinLock_);

            if (Terminated_) {
                return TError(
                    NRpc::EErrorCode::TransportError,
                    "Channel terminated")
                    << *EndpointAttributes_
                    << TerminationError_;
            }

            if (CurrentDiscoverySession_) {
                return CurrentDiscoverySession_;
            }

            if (!ActiveAddresses_.empty() || !PeersSetPromise_.IsSet()) {
                session = CurrentDiscoverySession_ = New<TDiscoverySession>(this);
            }
        }

        if (!session) {
            return MakeNoAlivePeersError();
        }

        PeersSetPromise_.ToFuture().Subscribe(
            BIND(&TImpl::OnPeersSet, MakeWeak(this)));
        session->GetFinished().Subscribe(
            BIND(&TImpl::OnDiscoverySessionFinished, MakeWeak(this)));
        return session;
    }

    TError MakeNoAlivePeersError()
    {
        auto guard = ReaderGuard(SpinLock_);
        if (PeerDiscoveryError_.IsOK()) {
            return TError(NRpc::EErrorCode::Unavailable, "No alive peers found")
                << *EndpointAttributes_;
        } else {
            return PeerDiscoveryError_;
        }
    }

    TError MakePeerDownError(const TString& address)
    {
        return TError("Peer %v is down", address)
            << *EndpointAttributes_;
    }

    TError MakePeerDiscoveryFailedError(const TString& address, const TError& error)
    {
        return TError("Discovery request failed for peer %v", address)
            << *EndpointAttributes_
            << error;
    }

    void OnPeersSet(const TError& /*error*/)
    {
        NTracing::TNullTraceContextGuard nullTraceContext;

        TDiscoverySessionPtr session;
        {
            auto guard = ReaderGuard(SpinLock_);

            YT_VERIFY(CurrentDiscoverySession_);
            session = CurrentDiscoverySession_;
        }

        session->Run();
    }

    void OnDiscoverySessionFinished(const TError& /*error*/)
    {
        NTracing::TNullTraceContextGuard nullTraceContext;
        auto guard = WriterGuard(SpinLock_);

        YT_VERIFY(CurrentDiscoverySession_);
        CurrentDiscoverySession_.Reset();

        TDelayedExecutor::CancelAndClear(RediscoveryCookie_);
        RediscoveryCookie_ = TDelayedExecutor::Submit(
            BIND(&TImpl::OnRediscovery, MakeWeak(this)),
            Config_->RediscoverPeriod + RandomDuration(Config_->RediscoverSplay));
    }

    void OnRediscovery(bool aborted)
    {
        if (aborted) {
            return;
        }

        Y_UNUSED(RunDiscoverySession());
    }

    void AddPeers(const std::vector<TString>& addresses)
    {
        auto guard = WriterGuard(SpinLock_);
        DoAddPeers(addresses);
    }

    void DoAddPeers(const std::vector<TString>& addresses)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        PeerDiscoveryError_ = {};

        std::vector<TString> newAddresses;
        for (const auto& address : addresses) {
            if (!BannedAddresses_.contains(address) && !ActiveAddresses_.contains(address)) {
                newAddresses.push_back(address);
            }
        }

        for (const auto& address : newAddresses) {
            AddPeer(address);
        }
    }

    void MaybeEvictRandomPeer()
    {
        ViablePeerRegistry_->MaybeRotateRandomPeer();
    }

    void AddPeer(const TString& address)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(ActiveAddresses_.insert(address).second);

        if (Config_->EnablePeerPolling) {
            auto poller = New<TPeerPoller>(this, address);
            poller->Run();
            YT_VERIFY(AddressToPoller_.emplace(address, std::move(poller)).second);
        }

        YT_LOG_DEBUG("Peer added (Address: %v)", address);
    }

    void RemovePeer(const TString& address)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        if (ActiveAddresses_.erase(address) == 0 && BannedAddresses_.erase(address) == 0) {
            return;
        }

        ViablePeerRegistry_->UnregisterPeer(address);

        if (Config_->EnablePeerPolling) {
            const auto& poller = GetOrCrash(AddressToPoller_, address);
            poller->Stop();

            YT_VERIFY(AddressToPoller_.erase(address));
        }

        YT_LOG_DEBUG("Peer removed (Address: %v)", address);
    }

    TPickPeerResult PickPeer(
        THashSet<TString>* requestingAddresses,
        THashSet<TString>* requestedAddresses)
    {
        auto guard = ReaderGuard(SpinLock_);

        if (std::ssize(*requestingAddresses) >= Config_->MaxConcurrentDiscoverRequests) {
            return TTooManyConcurrentRequests();
        }

        std::vector<TString> candidates;
        candidates.reserve(ActiveAddresses_.size());

        for (const auto& address : ActiveAddresses_) {
            if (requestingAddresses->find(address) == requestingAddresses->end() &&
                requestedAddresses->find(address) == requestedAddresses->end())
            {
                candidates.push_back(address);
            }
        }

        if (candidates.empty()) {
            return TNoMorePeers();
        }

        const auto& result = candidates[RandomNumber(candidates.size())];
        YT_VERIFY(requestedAddresses->insert(result).second);
        YT_VERIFY(requestingAddresses->insert(result).second);
        return result;
    }

    void BanPeer(const TString& address, TDuration backoffTime)
    {
        {
            auto guard = WriterGuard(SpinLock_);
            if (ActiveAddresses_.erase(address) != 1) {
                return;
            }
            BannedAddresses_.insert(address);
        }

        YT_LOG_DEBUG("Peer banned (Address: %v, BackoffTime: %v)",
            address,
            backoffTime);

        TDelayedExecutor::Submit(
            BIND(&TImpl::OnPeerBanTimeout, MakeWeak(this), address),
            backoffTime);
    }

    void UnbanPeer(const TString& address)
    {
        auto guard = WriterGuard(SpinLock_);
        if (BannedAddresses_.erase(address) != 1) {
            return;
        }
        ActiveAddresses_.insert(address);

        YT_LOG_DEBUG("Peer unbanned (Address: %v)", address);
    }

    void OnPeerBanTimeout(const TString& address, bool aborted)
    {
        if (aborted) {
            // If we are terminating -- do not unban anyone to prevent infinite retries.
            return;
        }

        UnbanPeer(address);
    }

    void AddViablePeer(const TString& address)
    {
        bool added = ViablePeerRegistry_->RegisterPeer(address);

        YT_LOG_DEBUG("Peer is viable (Address: %v, Added: %v)",
            address,
            added);
    }

    void InvalidatePeer(const TString& address)
    {
        ViablePeerRegistry_->UnregisterPeer(address);
    }

    void OnChannelFailed(
        const TString& address,
        const IChannelPtr& channel,
        const TError& error)
    {
        if (IsChannelFailureErrorHandled(error)) {
            YT_LOG_DEBUG(error, "Encountered already handled channel failure error (Address: %v)",
                address);
            return;
        }

        bool evicted = ViablePeerRegistry_->UnregisterChannel(address, channel);

        YT_LOG_DEBUG(
            error,
            "Peer is no longer viable due to channel failure (Address: %v, Evicted: %v)",
            address,
            evicted);
    }

    TGenericProxy CreateGenericProxy(IChannelPtr peerChannel)
    {
        auto serviceDescriptor = TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(GenericProtocolVersion);
        return TGenericProxy(std::move(peerChannel), serviceDescriptor);
    }

    IChannelPtr CreateChannel(const TString& address)
    {
        return CreateFailureDetectingChannel(
            ChannelFactory_->CreateChannel(address),
            Config_->AcknowledgementTimeout,
            BIND(&TImpl::OnChannelFailed, MakeWeak(this), address));
    }
};

////////////////////////////////////////////////////////////////////////////////

TDynamicChannelPool::TDynamicChannelPool(
    TDynamicChannelPoolConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    IAttributeDictionaryPtr endpointAttributes,
    std::string serviceName,
    TDiscoverRequestHook discoverRequestHook)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes),
        std::move(serviceName),
        std::move(discoverRequestHook)))
{ }

TDynamicChannelPool::~TDynamicChannelPool() = default;

TFuture<IChannelPtr> TDynamicChannelPool::GetRandomChannel()
{
    return Impl_->GetRandomChannel();
}

TFuture<IChannelPtr> TDynamicChannelPool::GetChannel(
    const IClientRequestPtr& request,
    const std::optional<THedgingChannelOptions>& hedgingOptions)
{
    return Impl_->GetChannel(request, hedgingOptions);
}

void TDynamicChannelPool::SetPeers(const std::vector<TString>& addresses)
{
    Impl_->SetPeers(addresses);
}

void TDynamicChannelPool::SetPeerDiscoveryError(const TError& error)
{
    Impl_->SetPeerDiscoveryError(error);
}

void TDynamicChannelPool::Terminate(const TError& error)
{
    Impl_->Terminate(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
