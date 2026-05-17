#pragma once

#include <yt/yt/core/bus/public.h>

#include <library/cpp/testing/common/network.h>

#include <string>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

struct TTcpBusTraits
{
    NTesting::TPortHolder Port;
    std::string Address;

    TTcpBusTraits();

    IBusServerPtr StartServer(IMessageHandlerPtr handler);
    IBusClientPtr CreateClient();
    IBusClientPtr CreateUnreachableClient();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
