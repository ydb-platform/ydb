#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/count_down_latch.h instead.

#include "public.h"

#include <library/cpp/yt/system/count_down_latch.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TCountDownLatch;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
