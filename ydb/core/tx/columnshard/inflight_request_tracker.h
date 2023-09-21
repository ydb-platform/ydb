#pragma once

#include "blob.h"
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NColumnShard {

using NOlap::TReadMetadata;
using NOlap::IBlobInUseTracker;

class TInFlightReadsTracker {
public:
    // Returns a unique cookie associated with this request
    ui64 AddInFlightRequest(NOlap::TReadMetadataBase::TConstPtr readMeta) {
        const ui64 cookie = NextCookie++;
        AddToInFlightRequest(cookie, readMeta);
        return cookie;
    }

    // Returns a unique cookie associated with this request
    template <class TReadMetadataList>
    ui64 AddInFlightRequest(const TReadMetadataList& readMetaList) {
        const ui64 cookie = NextCookie++;
        for (const auto& readMetaPtr : readMetaList) {
            AddToInFlightRequest(cookie, readMetaPtr);
        }
        return cookie;
    }

    void RemoveInFlightRequest(ui64 cookie) {
        Y_VERIFY(RequestsMeta.contains(cookie), "Unknown request cookie %" PRIu64, cookie);
        const auto& readMetaList = RequestsMeta[cookie];

        for (const auto& readMetaBase : readMetaList) {
            NOlap::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::TReadMetadata>(readMetaBase);

            if (!readMeta) {
                continue;
            }

            for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
                const ui64 portionId = portion->GetPortion();
                auto it = PortionUseCount.find(portionId);
                Y_VERIFY(it != PortionUseCount.end(), "Portion id %" PRIu64 " not found in request %" PRIu64, portionId, cookie);
                if (it->second == 1) {
                    PortionUseCount.erase(it);
                } else {
                    it->second--;
                }
                auto tracker = portion->GetBlobsStorage()->GetBlobsTracker();
                for (auto& rec : portion->Records) {
                    tracker->FreeBlob(rec.BlobRange.BlobId);
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

    // Checks if the portion is in use by any in-flight request
    bool IsPortionUsed(ui64 portionId) const {
        return PortionUseCount.contains(portionId);
    }

    NOlap::TSelectInfo::TStats GetSelectStatsDelta() {
        auto delta = SelectStatsDelta;
        SelectStatsDelta = NOlap::TSelectInfo::TStats();
        return delta;
    }

    TInFlightReadsTracker(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager)
        : StoragesManager(storagesManager)
    {

    }

private:
    void AddToInFlightRequest(const ui64 cookie, NOlap::TReadMetadataBase::TConstPtr readMetaBase) {
        RequestsMeta[cookie].push_back(readMetaBase);

        NOlap::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::TReadMetadata>(readMetaBase);

        if (!readMeta) {
            return;
        }

        auto selectInfo = readMeta->SelectInfo;
        Y_VERIFY(selectInfo);
        SelectStatsDelta += selectInfo->Stats();

        for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
            const ui64 portionId = portion->GetPortion();
            PortionUseCount[portionId]++;
            auto tracker = portion->GetBlobsStorage()->GetBlobsTracker();
            for (auto& rec : portion->Records) {
                tracker->UseBlob(rec.BlobRange.BlobId);
            }
        }

        auto insertStorage = StoragesManager->GetInsertOperator();
        auto tracker = insertStorage->GetBlobsTracker();
        for (const auto& committedBlob : readMeta->CommittedBlobs) {
            tracker->UseBlob(committedBlob.GetBlobRange().GetBlobId());
        }
    }

private:
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    ui64 NextCookie{1};
    THashMap<ui64, TList<NOlap::TReadMetadataBase::TConstPtr>> RequestsMeta;
    THashMap<ui64, ui64> PortionUseCount;
    NOlap::TSelectInfo::TStats SelectStatsDelta;
};

}
