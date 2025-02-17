#include "protocol_version.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TProtocolVersion TProtocolVersion::FromString(TStringBuf protocolVersionString)
{
    TStringBuf majorStr, minorStr;
    if (!protocolVersionString.TrySplit('.', majorStr, minorStr)) {
        THROW_ERROR_EXCEPTION("Failed to parse protocol version string; \"major.minor\" string expected");
    }

    TProtocolVersion result;

    if (!TryFromString<int>(majorStr, result.Major)) {
        THROW_ERROR_EXCEPTION("Failed to parse major protocol version");
    }

    if (!TryFromString<int>(minorStr, result.Minor)) {
        THROW_ERROR_EXCEPTION("Failed to parse minor protocol version");
    }

    if (result == GenericProtocolVersion) {
        return result;
    }

    if (result.Major < 0 || result.Minor < 0) {
        THROW_ERROR_EXCEPTION("Incorrect protocol version; major and minor versions should be "
            "greater than or equal to zero")
            << TErrorAttribute("protocol_version", ToString(result));
    }

    return result;
}

void FormatValue(TStringBuilderBase* builder, TProtocolVersion version, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v.%v", version.Major, version.Minor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
