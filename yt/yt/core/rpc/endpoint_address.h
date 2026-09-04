#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TParsedEndpointAddress
{
    TStringBuf Protocol;
    TStringBuf Address;
};

TParsedEndpointAddress ParseEndpointAddress(TStringBuf address);

struct TFormatEndpointAddressOptions
{
    bool OmitDefaultProtocol = false;
};

std::string FormatEndpointAddress(
    const TParsedEndpointAddress& parsedAddress,
    const TFormatEndpointAddressOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
