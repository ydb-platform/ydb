#include "remove.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsDeclareRemovingAction::DeclareRemove(const TUnifiedBlobId& blobId) {
    if (DeclaredBlobs.emplace(blobId).second) {
        ACFL_DEBUG("event", "DeclareRemove")("blob_id", blobId);
        return DoDeclareRemove(blobId);
    }
}

}
