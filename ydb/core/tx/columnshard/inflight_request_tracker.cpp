#include "inflight_request_tracker.h"
#include "engines/column_engine.h"
#include "engines/reader/plain_reader/constructor/read_metadata.h"

namespace NKikimr::NColumnShard {

void TInFlightReadsTracker::RemoveInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* index) {
    Y_ABORT_UNLESS(RequestsMeta.contains(cookie), "Unknown request cookie %" PRIu64, cookie);
    const auto& readMetaList = RequestsMeta[cookie];

    for (const auto& readMetaBase : readMetaList) {
        NOlap::NReader::NPlain::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

        if (!readMeta) {
            continue;
        }

        THashMap<TString, THashSet<NOlap::TUnifiedBlobId>> portionBlobIds;
        for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
            const ui64 portionId = portion->GetPortion();
            AFL_VERIFY(index);
            portion->FillBlobIdsByStorage(portionBlobIds, *index);
            auto it = PortionUseCount.find(portionId);
            Y_ABORT_UNLESS(it != PortionUseCount.end(), "Portion id %" PRIu64 " not found in request %" PRIu64, portionId, cookie);
            if (it->second == 1) {
                PortionUseCount.erase(it);
            } else {
                it->second--;
            }
        }

        for (auto&& i : portionBlobIds) {
            auto storage = StoragesManager->GetOperatorVerified(i.first);
            auto tracker = storage->GetBlobsTracker();
            for (auto& blobId : i.second) {
                tracker->FreeBlob(blobId);
            }
        }

        auto insertStorage = StoragesManager->GetInsertOperator();
        auto tracker = insertStorage->GetBlobsTracker();
        for (const auto& committedBlob : readMeta->CommittedBlobs) {
            tracker->FreeBlob(committedBlob.GetBlobRange().GetBlobId());
        }
    }

    RequestsMeta.erase(cookie);
}

TConclusionStatus TInFlightReadsTracker::AddToInFlightRequest(const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* index) {
    RequestsMeta[cookie].push_back(readMetaBase);

    auto readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

    if (!readMeta) {
        return TConclusionStatus::Success();
    }

    auto selectInfo = readMeta->SelectInfo;
    Y_ABORT_UNLESS(selectInfo);
    SelectStatsDelta += selectInfo->Stats();

    THashMap<TString, THashSet<NOlap::TUnifiedBlobId>> portionBlobIds;
    for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
        const ui64 portionId = portion->GetPortion();
        PortionUseCount[portionId]++;
        AFL_VERIFY(index);
        portion->FillBlobIdsByStorage(portionBlobIds, *index);
    }

    for (auto&& i : portionBlobIds) {
        auto storage = StoragesManager->GetOperatorOptional(i.first);
        if (!storage) {
            return TConclusionStatus::Fail("blobs storage info not ready for '" + i.first + "'");
        }
        auto tracker = storage->GetBlobsTracker();
        for (auto& blobId : i.second) {
            tracker->UseBlob(blobId);
        }
    }

    auto insertStorage = StoragesManager->GetInsertOperator();
    auto tracker = insertStorage->GetBlobsTracker();
    for (const auto& committedBlob : readMeta->CommittedBlobs) {
        tracker->UseBlob(committedBlob.GetBlobRange().GetBlobId());
    }
    return TConclusionStatus::Success();
}

}
