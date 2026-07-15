#pragma once

#include "public.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The wrapper over iStorage protects the underlying layers from executing
// overlapping zero or write requests.
// If a request is received that intersects with an already executing one, it is
// postponed until the completion of the executing one.
// If the new request is completely covered by the executing one, then such a
// request can not be executed, but wait for completion and respond to both
// requests to the client. This is possible because inflight requests can be
// reordered, and we pretend that the #2 request was executed first, and was
// then completely rewritten by #1.
// Read requests are not affected in any way.
// Thread-safe.

IStoragePtr CreateOverlappedRequestsGuardStorageWrapper(IStoragePtr storage);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
