#pragma once

#include <ydb/core/base/statestorage.h>
#include <ydb/core/discovery/discovery.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NReplication::NController {

class TNodesManager {
    static constexpr TDuration UpdateInternal = TDuration::Minutes(5);
    static constexpr TDuration RetryInternal = TDuration::Seconds(10);

public:
    struct TProcessResult {
        THashSet<ui32> NewNodes;
        THashSet<ui32> RemovedNodes;
    };

public:
    bool HasTenant(const TString& tenant) const;
    bool HasNodes(const TString& tenant) const;
    const THashSet<ui32>& GetNodes(const TString& tenant) const;
    ui32 GetRandomNode(const TString& tenant) const;

    void DiscoverNodes(const TString& tenant, const TActorId& cache, const TActorContext& ctx);
    TProcessResult ProcessResponse(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx);
    void ProcessResponse(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx);

    void Shutdown(const TActorContext& ctx);

private:
    THashMap<TString, THashSet<ui32>> TenantNodes;
    THashMap<TActorId, TString> NodeDiscoverers;
};

}
