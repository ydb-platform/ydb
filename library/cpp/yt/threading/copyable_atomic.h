#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/copyable_atomic.h instead.

#include <library/cpp/yt/system/copyable_atomic.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TCopyableAtomic;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
