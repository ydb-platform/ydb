#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/at_fork.h instead.

#include "writer_starving_rw_spin_lock.h"

#include <library/cpp/yt/system/at_fork.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::GetForkLock;
using ::NYT::RegisterAtForkHandlers;
using ::NYT::TAtForkHandler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
