#include "structured_payload.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

TStructuredLogEventPayload MakeStructuredPayloadFromYson(const NYson::TYsonString& message)
{
    return TStructuredLogEventPayload(message.ToSharedRef());
}

NYson::TYsonStringBuf GetYsonFromStructuredPayload(const TStructuredLogEventPayload& payload)
{
    return NYson::TYsonStringBuf(payload.Underlying().ToStringBuf(), NYson::EYsonType::MapFragment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
