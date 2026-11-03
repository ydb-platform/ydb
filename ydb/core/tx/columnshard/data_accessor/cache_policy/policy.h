#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/request_priority.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/meta.h>
#include <ydb/core/tx/general_cache/source/abstract.h>
#include <ydb/core/tx/general_cache/usage/service.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGeneralCache {

class TGlobalPortionAddress {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY(ui64, TabletId, 0);
    TPortionAddress InternalPortionAddress;
    YDB_READONLY(ui64, ContentStamp, 0);
    // Empty: the entry outlives the tablet actor. Otherwise only this actor can hit it.
    YDB_READONLY_DEF(NActors::TActorId, Incarnation);

public:
    const TPortionAddress& GetInternalPortionAddress() const;

    ui64 GetPortionId() const;

    TInternalPathId GetPathId() const;

    TGlobalPortionAddress(const NActors::TActorId& actorId, const ui64 tabletId, const TPortionAddress& internalAddress,
        const ui64 contentStamp = 0, const bool keepAcrossRestarts = true);

    TGlobalPortionAddress(const NActors::TActorId& actorId, const ui64 tabletId, const TPortionInfo& portion, const bool keepAcrossRestarts);

    bool operator==(const TGlobalPortionAddress& item) const;

    explicit operator size_t() const;

    const TString Debug() const;
};

class TPortionsMetadataCachePolicy {
public:
    using TAddress = TGlobalPortionAddress;
    using TObject = std::shared_ptr<TPortionDataAccessor>;
    using TSourceId = ui64;
    using EConsumer = NOlap::NBlobOperations::EConsumer;

    static bool EvictCacheWhenSourceGone(const TAddress& address) {
        return !!address.GetIncarnation();
    }

    static TSourceId GetSourceId(const TAddress& address);

    static NActors::TActorId GetRequestOwner(const TAddress& address) {
        return address.GetTabletActorId();
    }

    static bool IsCacheEntryActual(const TAddress& address, const TObject& object);

    static EConsumer DefaultConsumer();

    class TSizeCalcer {
    public:
        size_t operator()(const TObject& data);
    };

    static TString GetCacheName();

    static TString GetServiceCode();

    static std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>> BuildObjectsProcessor(
        const NActors::TActorId& serviceActorId);

    static NMemory::EMemoryConsumerKind GetConsumerKind();
};

}   // namespace NKikimr::NOlap::NGeneralCache

namespace NKikimr::NOlap::NDataAccessorControl {
using TGeneralCache = NKikimr::NGeneralCache::TServiceOperator<NKikimr::NOlap::NGeneralCache::TPortionsMetadataCachePolicy>;
}
