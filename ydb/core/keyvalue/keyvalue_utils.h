#pragma once

#include <ydb/core/keyvalue/keyvalue_events.h>

namespace NKikimr {
namespace NKeyValue {

NMsgBusProxy::EResponseStatus ConvertStatus(NKikimrKeyValue::Statuses::ReplyStatus status);

std::unique_ptr<IEventBase> MakeErrorResponse(const TIntermediate *intermediateResults, NMsgBusProxy::EResponseStatus status, TString errorDescription);

} // NKeyValue
} // NKikimr
