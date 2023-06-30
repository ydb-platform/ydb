#pragma once

#include "public.h"

#include "packet.h"

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Initializes a new client for communicating with a given address.
IBusClientPtr CreateBusClient(
    TBusClientConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
