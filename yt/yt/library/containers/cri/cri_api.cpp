#include "cri_api.h"

namespace NYT::NContainers::NCri {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCriRuntimeApi::TCriRuntimeApi(IChannelPtr channel)
    : TProxyBase(std::move(channel), GetDescriptor())
{ }

const TServiceDescriptor& TCriRuntimeApi::GetDescriptor()
{
    static const auto Descriptor = TServiceDescriptor(NProto::RuntimeService::service_full_name());
    return Descriptor;
}

////////////////////////////////////////////////////////////////////////////////

TCriImageApi::TCriImageApi(IChannelPtr channel)
    : TProxyBase(std::move(channel), GetDescriptor())
{ }

const TServiceDescriptor& TCriImageApi::GetDescriptor()
{
    static const auto Descriptor = TServiceDescriptor(NProto::ImageService::service_full_name());
    return Descriptor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
