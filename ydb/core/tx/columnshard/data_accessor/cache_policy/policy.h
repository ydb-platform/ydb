#pragma once
#include <ydb/core/tx/columnshard/engines/portions/meta.h>
#include <ydb/core/tx/general_cache/source/abstract.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGeneralCache {

class TGlobalPortionAddress {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY_DEF(TPortionAddress, InternalPortionAddress);

public:
    TGlobalPortionAddress(const NActors::TActorId& actorId, const TPortionAddress& internalAddress)
        : TabletActorId(actorId)
        , InternalPortionAddress(internalAddress) {
    }
};

class TPortionsMetadataCachePolicy {
public:
    using TAddress = TGlobalPortionAddress;
    using TObject = TPortionDataAccessor;
    enum class EConsumer {
        Compaction,
        Scan,
        Normalizer
    };

    class TSizeCalcer {
    public:
        size_t operator()(const TObject& data) {
            return sizeof(TAddress) + data.GetMetadataSize();
        }
    };

    static TString GetCacheName() {
        return "portions_metadata";
    }

    static TString GetServiceCode() {
        return "PRMT";
    }

    static std::shared_ptr<NGeneralCache::IObjectsProcessor> BuildObjectsProcessor(const NActors::TActorId& serviceActorId);
};

}   // namespace NKikimr::NOlap::NGeneralCache
