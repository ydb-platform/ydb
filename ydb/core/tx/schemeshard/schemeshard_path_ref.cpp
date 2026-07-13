#include "schemeshard_path_ref.h"

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TPathRef::Acquire() {
    if (SS) {
        SS->IncrementPathDbRefCount(PathId, Reason);
    }
}

void TPathRef::Release() {
    if (SS) {
        SS->DecrementPathDbRefCount(PathId, Reason);
        SS = nullptr;
    }
}

} // NKikimr::NSchemeShard
