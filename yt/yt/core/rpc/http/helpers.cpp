#include "helpers.h"

#include <yt/yt/core/http/helpers.h>

namespace NYT::NRpc::NHttp {

using namespace NYT::NHttp::NHeaders;

////////////////////////////////////////////////////////////////////////////////

std::string ToHttpContentType(EMessageFormat format)
{
    switch (format) {
        case EMessageFormat::Protobuf:
            return ApplicationXProtobufContentType;
        case EMessageFormat::Json:
            return ApplicationJsonContentType + ";charset=utf8";
        case EMessageFormat::Yson:
            return ApplicationXYsonContentType;
        default:
            YT_ABORT();
    }
}

std::optional<EMessageFormat> FromHttpContentType(TStringBuf contentType)
{
    if (contentType == ApplicationXProtobufContentType) {
        return EMessageFormat::Protobuf;
    } else if (contentType.StartsWith(ApplicationJsonContentType)) {
        return EMessageFormat::Json;
    } else if (contentType == ApplicationXYsonContentType) {
        return EMessageFormat::Yson;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
