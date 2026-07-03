#pragma once

#include <yt/yt/core/bus/public.h>

#include <library/cpp/testing/common/network.h>

#include <string>

namespace NYT::NBus::NTcp::NTests {

////////////////////////////////////////////////////////////////////////////////

struct TBusTraits
{
    NTesting::TPortHolder Port;
    std::string Address;

    TBusTraits();

    IBusServerPtr StartServer(IMessageHandlerPtr handler);
    IBusClientPtr CreateClient();
    IBusClientPtr CreateUnreachableClient();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp::NTests
