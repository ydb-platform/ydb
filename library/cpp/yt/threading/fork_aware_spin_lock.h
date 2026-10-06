#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/fork_aware_spin_lock.h instead.

#include "public.h"
#include "spin_lock.h"

#include <library/cpp/yt/system/fork_aware_spin_lock.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TForkAwareSpinLock;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
