#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TSharedRef ConvertMessageToFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const NYson::TProtobufMessageType* messageType,
    const NYson::TYsonString& formatOptionsYson);

TSharedRef ConvertMessageFromFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const NYson::TProtobufMessageType* messageType,
    const NYson::TYsonString& formatOptionsYson,
    bool enveloped);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
