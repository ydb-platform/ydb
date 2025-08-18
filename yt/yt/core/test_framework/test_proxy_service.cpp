#include "test_proxy_service.h"

namespace NYT::NRpc {

using namespace NYTree;
using namespace ::NYT::NBus;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TTestChannelFactory::TTestChannelFactory(
    THashMap<std::string, TRealmIdServiceMap> addressToServices,
    TRealmIdServiceMap defaultServices)
    : AddressToServices_(std::move(addressToServices))
    , DefaultServices_(std::move(defaultServices))
{ }

IChannelPtr TTestChannelFactory::CreateChannel(const std::string& address)
{
    return New<TTestChannel>(GetOrDefault(AddressToServices_, address, {}), DefaultServices_, address);
}

////////////////////////////////////////////////////////////////////////////////

TTestChannel::TTestChannel(
    TRealmIdServiceMap services,
    TRealmIdServiceMap defaultServices,
    const std::string& address)
    : Services_(std::move(services))
    , DefaultServices_(std::move(defaultServices))
    , Address_(address)
    , Attributes_(ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("address").Value(Address_)
        .EndMap()))
{ }

const std::string& TTestChannel::GetEndpointDescription() const
{
    return Address_;
}

const IAttributeDictionary& TTestChannel::GetEndpointAttributes() const
{
    return *Attributes_;
}

const IServicePtr& TTestChannel::GetServiceOrThrow(const TServiceId& serviceId) const
{
    const auto& realmId = serviceId.RealmId;
    const auto& serviceName = serviceId.ServiceName;
    auto& services = Services_.empty() ? DefaultServices_ : Services_;
    auto serviceMapIt = services.find(realmId);

    if (serviceMapIt == services.end()) {
        if (realmId) {
            auto innerError = TError(NRpc::EErrorCode::NoSuchRealm, "Request realm is unknown")
                << TErrorAttribute("service", serviceName)
                << TErrorAttribute("realm_id", realmId);
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::NoSuchService,
                "Service is not registered")
                << innerError;
        } else {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::NoSuchService,
                "Service is not registered")
                << TErrorAttribute("service", serviceName)
                << TErrorAttribute("realm_id", realmId);
        }
    }
    auto& serviceMap = serviceMapIt->second;
    auto serviceIt = serviceMap.find(serviceName);
    if (serviceIt == serviceMap.end()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::NoSuchService,
            "Service is not registered")
            << TErrorAttribute("service", serviceName)
            << TErrorAttribute("realm_id", realmId);
    }

    return serviceIt->second;
}

void TTestChannel::HandleRequestResult(
    const std::string& address,
    TGuid requestId,
    IClientResponseHandlerPtr response,
    const TError& error)
{
    auto busIt = RequestToBus_.find(std::pair(address, requestId));
    auto bus = busIt->second;

    if (error.IsOK() && bus->GetMessage().Size() >= 2) {
        response->HandleResponse(bus->GetMessage(), address);
    } else if (error.IsOK()) {
        NProto::TResponseHeader header;
        YT_VERIFY(TryParseResponseHeader(bus->GetMessage(), &header));
        auto wrappedError = TError("Test proxy service error")
            << FromProto<TError>(header.error());
        response->HandleError(std::move(wrappedError));
    } else {
        auto wrappedError = TError("Test proxy service error")
            << error;
        response->HandleError(std::move(wrappedError));
    }

    RequestToBus_.erase(busIt);
}

IClientRequestControlPtr TTestChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    const TSendOptions& /*options*/)
{
    TServiceId serviceId(request->GetService(), request->GetRealmId());
    auto service = GetServiceOrThrow(serviceId);
    auto requestId = request->GetRequestId();
    auto requestControl = New<TTestClientRequestControl>(
        service,
        requestId);

    auto bus = New<TTestBus>(Address_);
    EmplaceOrCrash(RequestToBus_, std::pair(Address_, requestId), bus);

    try {
        // Serialization modifies the request header and should be called prior to header copying.
        auto serializedMessage = request->Serialize();
        service->HandleRequest(
            std::make_unique<NProto::TRequestHeader>(request->Header()),
            std::move(serializedMessage),
            bus);
        bus->GetReadyResponseFuture()
            .Subscribe(BIND(&TTestChannel::HandleRequestResult, MakeStrong(this), Address_, requestId, responseHandler));
    } catch (const std::exception& ex) {
        HandleRequestResult(Address_, requestId, responseHandler, ex);
    }

    return requestControl;
}

void TTestChannel::Terminate(const TError& error)
{
    YT_VERIFY(!error.IsOK());
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (TerminationFlag_.exchange(true)) {
        return;
    }

    TerminationError_.Store(error);
    Terminated_.Fire(error);
}

void TTestChannel::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Subscribe(callback);
}

void TTestChannel::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Unsubscribe(callback);
}

int TTestChannel::GetInflightRequestCount()
{
    return 0;
}

const IMemoryUsageTrackerPtr& TTestChannel::GetChannelMemoryTracker()
{
    return MemoryUsageTracker_;
}

////////////////////////////////////////////////////////////////////////////////

TTestBus::TTestBus(const std::string& address)
    : Address_(address)
    , EndpointDescription_(address)
    , Attributes_(ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("address").Value(Address_)
        .EndMap()))
    , NetworkAddress_(NNet::TNetworkAddress())
{ }

const std::string& TTestBus::GetEndpointDescription() const
{
    return EndpointDescription_;
}

const NYTree::IAttributeDictionary& TTestBus::GetEndpointAttributes() const
{
    return *Attributes_;
}

const std::string& TTestBus::GetEndpointAddress() const
{
    return Address_;
}

const NNet::TNetworkAddress& TTestBus::GetEndpointNetworkAddress() const
{
    return NetworkAddress_;
}

TBusNetworkStatistics TTestBus::GetNetworkStatistics() const
{
    return TBusNetworkStatistics{};
}

bool TTestBus::IsEndpointLocal() const
{
    return false;
}

bool TTestBus::IsEncrypted() const
{
    return false;
}

TFuture<void> TTestBus::GetReadyFuture() const
{
    return VoidFuture;
}

TFuture<void> TTestBus::Send(TSharedRefArray message, const ::NYT::NBus::TSendOptions& /*options*/)
{
    YT_VERIFY(Message_.Empty() && !ReadyPromise_.IsSet());

    Message_ = message;
    ReadyPromise_.TrySet();
    return ReadyPromise_.ToFuture();
}

void TTestBus::SetTosLevel(TTosLevel /*tosLevel*/)
{
    // Do nothing.
}

void TTestBus::Terminate(const TError& error)
{
    ReadyPromise_.TrySet(error);
}

TSharedRefArray TTestBus::GetMessage() const
{
    return Message_;
}

TFuture<void> TTestBus::GetReadyResponseFuture() const
{
    return ReadyPromise_.ToFuture();
}

void TTestBus::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Subscribe(callback);
}

void TTestBus::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Unsubscribe(callback);
}

////////////////////////////////////////////////////////////////////////////////

TTestClientRequestControl::TTestClientRequestControl(IServicePtr service, TRequestId requestId)
    : Service_(std::move(service))
    , RequestId_(requestId)
{ }

void TTestClientRequestControl::Cancel()
{
    Service_->HandleRequestCancellation(RequestId_);
}

TFuture<void> TTestClientRequestControl::SendStreamingPayload(const TStreamingPayload& payload)
{
    Service_->HandleStreamingPayload(RequestId_, payload);
    return VoidFuture;
}

TFuture<void> TTestClientRequestControl::SendStreamingFeedback(const TStreamingFeedback& feedback)
{
    Service_->HandleStreamingFeedback(RequestId_, feedback);
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
