#include "remove.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsDeclareRemovingAction::DeclareRemove(const TUnifiedBlobId& blobId) {
    if (DeclaredBlobs.emplace(blobId).second) {
        ACFL_DEBUG("event", "DeclareRemove")("blob_id", blobId);
        Counters->OnRequest(blobId.BlobSize());
        return DoDeclareRemove(blobId);
    }
}

}
