#include "test_server_host.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTestServerHost::TTestServerHost(
    NTesting::TPortHolder port,
    NRpc::IServerPtr server,
    std::vector<NRpc::IServicePtr> services,
    TTestNodeMemoryTrackerPtr memoryUsageTracker)
    : Port_(std::move(port))
    , Server_(std::move(server))
    , Services_(std::move(services))
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
{
    InitializeServer();
}

void TTestServerHost::InitializeServer()
{
    for (const auto& service : Services_) {
        Server_->RegisterService(service);
    }

    Server_->Start();
}

void TTestServerHost::TearDown()
{
    Server_->Stop().Get().ThrowOnError();
    Server_.Reset();
    Port_.Reset();
}

TTestNodeMemoryTrackerPtr TTestServerHost::GetMemoryUsageTracker() const
{
    return MemoryUsageTracker_;
}

TString TTestServerHost::GetAddress() const
{
    return Format("localhost:%v", static_cast<ui16>(Port_));
}

std::vector<NRpc::IServicePtr> TTestServerHost::GetServices() const
{
    return Services_;
}

NRpc::IServerPtr TTestServerHost::GetServer() const
{
    return Server_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
