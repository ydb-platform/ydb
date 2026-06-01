#include "dispatcher.h"

#include "dispatcher_impl.h"

#include <yt/yt/core/bus/private.h>

namespace NYT::NBus::NTcp {

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(New<TImpl>())
{
    BusProfiler().WithSparse().AddProducer("", Impl_);
}

TDispatcher* TDispatcher::Get()
{
    return LeakySingleton<TDispatcher>();
}

void TDispatcher::Configure(const TDispatcherConfigPtr& config)
{
    Impl_->Configure(config);
}

const TBusNetworkCountersPtr& TDispatcher::GetCounters(const TString& networkName, bool encrypted)
{
    return Impl_->GetCounters(networkName, encrypted);
}

NConcurrency::IPollerPtr TDispatcher::GetXferPoller()
{
    return Impl_->GetXferPoller();
}

void TDispatcher::DisableNetworking()
{
    Impl_->DisableNetworking();
}

bool TDispatcher::IsNetworkingDisabled()
{
    return Impl_->IsNetworkingDisabled();
}

const std::string& TDispatcher::GetNetworkNameForAddress(const NNet::TNetworkAddress& address)
{
    return Impl_->GetNetworkNameForAddress(address);
}

TTosLevel TDispatcher::GetTosLevelForBand(EMultiplexingBand band)
{
    return Impl_->GetTosLevelForBand(band);
}

int TDispatcher::GetMultiplexingParallelism(EMultiplexingBand band, int multiplexingParallelism)
{
    return Impl_->GetMultiplexingParallelism(band, multiplexingParallelism);
}

NYTree::IYPathServicePtr TDispatcher::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp
