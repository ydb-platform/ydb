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

void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason) {
    if (ss) {
        ss->IncrementPathDbRefCount(pathId, reason);
    }
}

void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason) {
    if (ss) {
        ss->DecrementPathDbRefCount(pathId, reason);
    }
}

} // NKikimr::NSchemeShard
