#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/meta.h>
#include <ydb/core/tx/general_cache/source/abstract.h>
#include <ydb/core/tx/general_cache/usage/service.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGeneralCache {

class TGlobalPortionAddress {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    TPortionAddress InternalPortionAddress;

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

    TGlobalPortionAddress(const NActors::TActorId& actorId, const TPortionAddress& internalAddress)
        : TabletActorId(actorId)
        , InternalPortionAddress(internalAddress) {
    }

    bool operator==(const TGlobalPortionAddress& item) const {
        return TabletActorId == item.TabletActorId && InternalPortionAddress == item.InternalPortionAddress;
    }

    explicit operator size_t() const {
        return TabletActorId.Hash() ^ THash<NKikimr::NOlap::TPortionAddress>()(InternalPortionAddress);
    }
};

class TPortionsMetadataCachePolicy {
public:
    using TAddress = TGlobalPortionAddress;
    using TObject = std::shared_ptr<TPortionDataAccessor>;
    using TSourceId = NActors::TActorId;
    using EConsumer = NOlap::NBlobOperations::EConsumer;

    static TSourceId GetSourceId(const TAddress& address) {
        return address.GetTabletActorId();
    }

    static EConsumer DefaultConsumer() {
        return EConsumer::UNDEFINED;
    }

    class TSizeCalcer {
    public:
        size_t operator()(const TObject& data) {
            AFL_VERIFY(data);
            return sizeof(TAddress) + data->GetMetadataSize();
        }
    };

    static TString GetCacheName() {
        return "portions_metadata";
    }

    static TString GetServiceCode() {
        return "PRMT";
    }

    static std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>> BuildObjectsProcessor(
        const NActors::TActorId& serviceActorId);

    static NMemory::EMemoryConsumerKind GetConsumerKind() {
        return NMemory::EMemoryConsumerKind::ColumnTablesDataAccessorCache;
    }
};

}   // namespace NKikimr::NOlap::NGeneralCache

namespace NKikimr::NOlap::NDataAccessorControl {
    using TGeneralCache = NKikimr::NGeneralCache::TServiceOperator<NKikimr::NOlap::NGeneralCache::TPortionsMetadataCachePolicy>;
}
