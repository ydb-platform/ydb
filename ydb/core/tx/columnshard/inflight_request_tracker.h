#pragma once

#include "blob.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap {
class TVersionedIndex;
}

namespace NKikimr::NColumnShard {

using NOlap::IBlobInUseTracker;

class TInFlightReadsTracker {
public:
    // Returns a unique cookie associated with this request
    ui64 AddInFlightRequest(NOlap::NReader::TReadMetadataBase::TConstPtr readMeta, const NOlap::TVersionedIndex* index) {
        const ui64 cookie = NextCookie++;
        AddToInFlightRequest(cookie, readMeta, index);
        return cookie;
    }

    // Returns a unique cookie associated with this request
    template <class TReadMetadataList>
    ui64 AddInFlightRequest(const TReadMetadataList& readMetaList, const NOlap::TVersionedIndex* index) {
        const ui64 cookie = NextCookie++;
        for (const auto& readMetaPtr : readMetaList) {
            AddToInFlightRequest(cookie, readMetaPtr, index);
        }
        return cookie;
    }

    void RemoveInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* index);

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
    void AddToInFlightRequest(const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* index);

private:
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    ui64 NextCookie{1};
    THashMap<ui64, TList<NOlap::NReader::TReadMetadataBase::TConstPtr>> RequestsMeta;
    THashMap<ui64, ui64> PortionUseCount;
    NOlap::TSelectInfo::TStats SelectStatsDelta;
};

}
