#pragma once

// TODO(babenko): Drop this shim; include library/cpp/yt/system/rw_spin_lock.h instead.

#include "public.h"
#include "spin_lock_base.h"
#include "spin_lock_count.h"
#include "spin_wait.h"

#include <library/cpp/yt/memory/public.h>

#include <library/cpp/yt/system/rw_spin_lock.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::ForkFriendlyReaderGuard;
using ::NYT::ReaderGuard;
using ::NYT::TForkFriendlyReaderSpinlockTraits;
using ::NYT::TPaddedReaderWriterSpinLock;
using ::NYT::TReaderGuard;
using ::NYT::TReaderSpinlockTraits;
using ::NYT::TReaderWriterSpinLock;
using ::NYT::TWriterGuard;
using ::NYT::TWriterSpinlockTraits;
using ::NYT::WriterGuard;

namespace NDetail {

using ::NYT::NDetail::TCheckedReaderWriterSpinLock;
using ::NYT::NDetail::TUncheckedReaderWriterSpinLock;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
