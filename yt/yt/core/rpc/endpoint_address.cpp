#include "endpoint_address.h"

#include <library/cpp/yt/string/string.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

constexpr auto ProtocolDesignator = "://"_sb;

TParsedEndpointAddress ParseEndpointAddress(TStringBuf address)
{
    auto pos = address.find(ProtocolDesignator);
    return pos == TStringBuf::npos
        ? TParsedEndpointAddress{
            .Protocol = DefaultProtocolName,
            .Address = address,
        }
        : TParsedEndpointAddress{
            .Protocol = address.substr(0, pos),
            .Address = address.substr(pos + ProtocolDesignator.length()),
        };
}

std::string FormatEndpointAddress(
    const TParsedEndpointAddress& parsedAddress,
    const TFormatEndpointAddressOptions& options)
{
    if (options.OmitDefaultProtocol && parsedAddress.Protocol == DefaultProtocolName) {
        return std::string(parsedAddress.Address);
    }
    return ConcatToString(
        parsedAddress.Protocol,
        ProtocolDesignator,
        parsedAddress.Address);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
