#include "helpers.h"

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

TString ToHttpContentType(EMessageFormat format)
{
    switch (format) {
        case EMessageFormat::Protobuf:
            return "application/x-protobuf";
        case EMessageFormat::Json:
            return "application/json;charset=utf8";
        case EMessageFormat::Yson:
            return "application/x-yson";
        default:
            YT_ABORT();
    }
}

std::optional<EMessageFormat> FromHttpContentType(TStringBuf contentType)
{
    if (contentType == "application/x-protobuf") {
        return EMessageFormat::Protobuf;
    } else if (contentType.StartsWith("application/json")) {
        return EMessageFormat::Json;
    } else if (contentType == "application/x-yson") {
        return EMessageFormat::Yson;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
