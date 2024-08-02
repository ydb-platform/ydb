#pragma once

#include "public.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TTestChannelFactory
    : public IChannelFactory
{
public:
    TTestChannelFactory(
        THashMap<TString, TRealmIdServiceMap> addressToServices,
        TRealmIdServiceMap defaultServices);

    IChannelPtr CreateChannel(const TString& address) override;

private:
    const THashMap<TString, TRealmIdServiceMap> AddressToServices_;
    const TRealmIdServiceMap DefaultServices_;
};

DEFINE_REFCOUNTED_TYPE(TTestChannelFactory);

////////////////////////////////////////////////////////////////////////////////

class TTestBus
    : public ::NYT::NBus::IBus
{
public:
    explicit TTestBus(TString address);

    const TString& GetEndpointDescription() const override;

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    const TString& GetEndpointAddress() const override;

    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override;

    ::NYT::NBus::TBusNetworkStatistics GetNetworkStatistics() const override;

    bool IsEndpointLocal() const override;

    bool IsEncrypted() const override;

    TFuture<void> GetReadyFuture() const override;

    TFuture<void> Send(TSharedRefArray message, const ::NYT::NBus::TSendOptions& options = {}) override;

    void SetTosLevel(::NYT::NBus::TTosLevel tosLevel) override;

    void Terminate(const TError& error) override;

    TSharedRefArray GetMessage() const;

    TFuture<void> GetReadyResponseFuture() const;

    DECLARE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

private:
    const TString Address_;
    const NYTree::IAttributeDictionaryPtr Attributes_;
    const NNet::TNetworkAddress NetworkAddress_;

    TPromise<void> ReadyPromise_ = NewPromise<void>();
    TSharedRefArray Message_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;
};

DEFINE_REFCOUNTED_TYPE(TTestBus)

////////////////////////////////////////////////////////////////////////////////

class TTestChannel
    : public IChannel
{
public:
    TTestChannel(
        TRealmIdServiceMap services,
        TRealmIdServiceMap defaultServices,
        TString address);

    const TString& GetEndpointDescription() const override;

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override;

    void Terminate(const TError& error) override;

    int GetInflightRequestCount() override;

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override;

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

private:
    const TRealmIdServiceMap Services_;
    const TRealmIdServiceMap DefaultServices_;
    const TString Address_;
    const NYTree::IAttributeDictionaryPtr Attributes_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_ = GetNullMemoryUsageTracker();

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    std::atomic<bool> TerminationFlag_ = false;
    TAtomicObject<TError> TerminationError_;

    THashMap<std::pair<TString, TGuid>, TTestBusPtr> RequestToBus_;

    void HandleRequestResult(
        TString address,
        TGuid requestId,
        IClientResponseHandlerPtr responseHandler,
        const TError& error);

    const IServicePtr& GetServiceOrThrow(const TServiceId& serviceId) const;
};

DEFINE_REFCOUNTED_TYPE(TTestChannel);

////////////////////////////////////////////////////////////////////////////////

class TTestClientRequestControl
    : public IClientRequestControl
{
public:
    TTestClientRequestControl(IServicePtr service, TRequestId requestId);

    void Cancel() override;

    TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override;

    TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override;

private:
    const IServicePtr Service_;
    const TRequestId RequestId_;
};

DEFINE_REFCOUNTED_TYPE(TTestClientRequestControl);

////////////////////////////////////////////////////////////////////////////////

template <class TAddressContainer, class TDefaultContainer>
IChannelFactoryPtr CreateTestChannelFactory(
    const THashMap<TString, TAddressContainer>& addressToServices,
    const TDefaultContainer& defaultServices = {});

template <class TDefaultContainer>
IChannelFactoryPtr CreateTestChannelFactoryWithDefaultServices(const TDefaultContainer& defaultServices);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define TEST_PROXY_SERVICE_INL_H_
#include "test_proxy_service-inl.h"
#undef TEST_PROXY_SERVICE_INL_H_
