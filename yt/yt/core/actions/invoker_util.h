#pragma once

#include "invoker.h"
#include "public.h"

#include <yt/yt/core/actions/bind.h>

// TODO: Many TUs transitively relied on finally.h via the removed
// invoker_util-inl.h; keep it reachable here until they include it directly.
#include <yt/yt/core/misc/finally.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declaration
IInvoker* GetCurrentInvoker();

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

//! Returns a callback that runs #onSuccess when invoked or #onCancel when
//! destroyed without having been invoked (e.g. discarded by an invoker).
//! Must be invoked at most once.
TClosure MakeGuardedCallback(
    TClosure onSuccess,
    TClosure onCancel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
