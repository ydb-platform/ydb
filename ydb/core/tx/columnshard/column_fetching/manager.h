#pragma once

#include "cache_policy.h"

namespace NKikimr::NOlap::NColumnFetching {

class TColumnDataManager {
private:
    TActorId ColumnShardActorId;

public:
    TColumnDataManager(const TActorId& columnShardActorId)
        : ColumnShardActorId(columnShardActorId)
    {
    }

    void AskColumnData(const NBlobOperations::EConsumer consumer, const THashSet<TPortionAddress>& portions, const std::set<ui32>& columns,
        std::shared_ptr<::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy>>&& callback) {
        THashSet<NGeneralCache::TGlobalColumnAddress> addresses;
        for (const auto& portion : portions) {
            for (const auto& column : columns) {
                addresses.insert(NGeneralCache::TGlobalColumnAddress(ColumnShardActorId, portion, column));
            }
        }
        NColumnFetching::TGeneralCache::AskObjects(consumer, std::move(addresses), std::move(callback));
    }
};

}   // namespace NKikimr::NOlap::NColumnFetching
