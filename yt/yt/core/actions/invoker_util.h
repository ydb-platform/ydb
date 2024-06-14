#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Returns the synchronous-ish invoker that defers recurrent action invocation.
/*!
 *  The invoker's |Invoke| method invokes the closure immediately unless invoking
 *  code is already running within an action in a sync invoker. In latter case all
 *  subsequent actions are put into the deferred action queue and are executed one
 *  by one in synchronous manner after completion of the outermost action.
 *  This is quite similar to BFS over the recursion tree.
 *
 *  Such implementation ensures that Subscribe chains over futures from sync invoker
 *  do not lead to an unbounded recursion. Note that the invocation order is slightly
 *  different from the "truly synchronous" invoker that always invokes actions synchronously,
 *  which corresponds to DFS over the recursion tree.
 */
IInvokerPtr GetSyncInvoker();

//! Returns the null invoker, i.e. the invoker whose |Invoke|
//! method does nothing.
IInvokerPtr GetNullInvoker();

//! Returns a special per-process invoker that handles all asynchronous finalization
//! activities (fiber unwinding, abandoned promise cancelation etc).
IInvokerPtr GetFinalizerInvoker();

//! Tries to invoke #onSuccess via #invoker.
//! If the invoker discards the callback without executing it then
//! #onCancel is run.
template <CInvocable<void()> TOnSuccess, CInvocable<void()> TOnCancel>
void GuardedInvoke(
    const IInvokerPtr& invoker,
    TOnSuccess onSuccess,
    TOnCancel onCancel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INVOKER_UTIL_INL_H_
#include "invoker_util-inl.h"
#undef INVOKER_UTIL_INL_H_
