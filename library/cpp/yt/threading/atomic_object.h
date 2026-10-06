#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/atomic_object.h instead.

#include "writer_starving_rw_spin_lock.h"

#include <library/cpp/yt/system/atomic_object.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::TAtomicObject;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
