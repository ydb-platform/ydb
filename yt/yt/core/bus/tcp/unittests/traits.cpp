#include "traits.h"

#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

namespace NYT::NBus::NTcp::NTests {

////////////////////////////////////////////////////////////////////////////////

TBusTraits::TBusTraits()
    : Port(NTesting::GetFreePort())
    , Address(Format("localhost:%v", Port))
{ }

NBus::IBusServerPtr TBusTraits::StartServer(IMessageHandlerPtr handler)
{
    auto config = TBusServerConfig::CreateTcp(Port);
    auto server = CreateBusServer(config);
    server->Start(std::move(handler));
    return server;
}

NBus::IBusClientPtr TBusTraits::CreateClient()
{
    return CreateBusClient(TBusClientConfig::CreateTcp(Address));
}

NBus::IBusClientPtr TBusTraits::CreateUnreachableClient()
{
    auto unreachablePort = NTesting::GetFreePort();
    return CreateBusClient(TBusClientConfig::CreateTcp(
        Format("localhost:%v", unreachablePort)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp::NTests
