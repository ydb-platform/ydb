#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/fork_aware_rw_spin_lock.h instead.

#include "rw_spin_lock.h"

#include <library/cpp/yt/system/fork_aware_rw_spin_lock.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TForkAwareReaderWriterSpinLock;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
