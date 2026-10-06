#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/notification_handle.h instead.

#include "public.h"

#include <library/cpp/yt/system/notification_handle.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TNotificationHandle;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
