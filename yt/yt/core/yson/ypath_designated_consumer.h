#pragma once

#include "consumer.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMissingPathMode,
    (ThrowError)
    (Ignore)
    (EmitEntity)
);

////////////////////////////////////////////////////////////////////////////////

//! Creates a YSON consumer that navigates #path inside the incoming YSON and
//! routes the value found there into #underlyingConsumer.
//!
//! #Flush both finalizes the current document — emitting a single #OnEntity to
//! the underlying consumer when the path missed and #missingPathMode is
//! #EmitEntity — and resets per-document state, so the same instance can be
//! fed another YSON node afterwards.
std::unique_ptr<IFlushableYsonConsumer> CreateYPathDesignatedConsumer(
    NYPath::TYPath path,
    EMissingPathMode missingPathMode,
    NYson::IYsonConsumer* underlyingConsumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
