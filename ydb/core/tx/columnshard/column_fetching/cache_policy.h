#pragma once
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/meta.h>
#include <ydb/core/tx/general_cache/source/abstract.h>
#include <ydb/core/tx/general_cache/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGeneralCache {

class TGlobalColumnAddress {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    TPortionAddress InternalPortionAddress;
    YDB_READONLY_DEF(ui32, ColumnId);

public:
    const TPortionAddress& GetInternalPortionAddress() const {
        return InternalPortionAddress;
    }

    ui64 GetPortionId() const {
        return InternalPortionAddress.GetPortionId();
    }

    TInternalPathId GetPathId() const {
        return InternalPortionAddress.GetPathId();
    }

    TGlobalColumnAddress(const NActors::TActorId& actorId, const TPortionAddress& internalAddress, const ui64 columnId)
        : TabletActorId(actorId)
        , InternalPortionAddress(internalAddress)
        , ColumnId(columnId)
    {
    }

    bool operator==(const TGlobalColumnAddress& item) const {
        return TabletActorId == item.TabletActorId && InternalPortionAddress == item.InternalPortionAddress && ColumnId == item.ColumnId;
    }

    explicit operator size_t() const {
        ui64 h = 0;
        h = CombineHashes(h, TabletActorId.Hash());
        h = CombineHashes(h, THash<NKikimr::NOlap::TPortionAddress>()(InternalPortionAddress));
        h = CombineHashes(h, (ui64)ColumnId);
        return h;
    }
};

class TColumnDataCachePolicy {
public:
    using TAddress = TGlobalColumnAddress;
    using TObject = std::shared_ptr<NArrow::NAccessor::IChunkedArray>;
    using EConsumer = NOlap::NBlobOperations::EConsumer;
    using TSourceId = TActorId;

    static EConsumer DefaultConsumer() {
        return EConsumer::UNDEFINED;
    }

    class TSizeCalcer {
    public:
        size_t operator()(const TObject& data) {
            return sizeof(TAddress) + sizeof(TObject) + data->GetRawSizeVerified();
        }
    };

    static TString GetCacheName() {
        return "column_data";
    }

    static TString GetServiceCode() {
        return "CLMN";
    }

    static std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TColumnDataCachePolicy>> BuildObjectsProcessor(
        const NActors::TActorId& serviceActorId);

    static NMemory::EMemoryConsumerKind GetConsumerKind() {
        return NMemory::EMemoryConsumerKind::ColumnDataCache;
    }
};

}   // namespace NKikimr::NOlap::NGeneralCache

namespace NKikimr::NOlap::NColumnFetching {
using TGeneralCache = NKikimr::NGeneralCache::TServiceOperator<NKikimr::NOlap::NGeneralCache::TColumnDataCachePolicy>;
}
