#include "schemeshard_path_ref.h"

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TPathRef::Acquire() {
    if (SS) {
        SS->IncrementPathDbRefCount(PathId, Reason.c_str());
    }
}

void TPathRef::Release() {
    if (SS) {
        SS->DecrementPathDbRefCount(PathId, Reason.c_str());
        SS = nullptr;
    }
}

void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason) {
    if (ss) {
        ss->IncrementPathDbRefCount(pathId, reason.c_str());
    }
}

void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason) {
    if (ss) {
        ss->DecrementPathDbRefCount(pathId, reason.c_str());
    }
}

} // NKikimr::NSchemeShard
