#pragma once

#define TRACELESS_GUARD_INL_H_
#include "traceless_guard-inl.h"
#undef TRACELESS_GUARD_INL_H_

namespace NYT::NThreading {

// This guards are zero-cost replacements for normal ones
// which allow user to avoid spinlocks being tracked.

////////////////////////////////////////////////////////////////////////////////

using NPrivate::TTracelessGuard;
using NPrivate::TTracelessInverseGuard;
using NPrivate::TTracelessTryGuard;
using NPrivate::TTracelessReaderGuard;
using NPrivate::TTracelessWriterGuard;

////////////////////////////////////////////////////////////////////////////////

using NPrivate::TracelessGuard;
using NPrivate::TracelessTryGuard;
using NPrivate::TracelessReaderGuard;
using NPrivate::TracelessWriterGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
