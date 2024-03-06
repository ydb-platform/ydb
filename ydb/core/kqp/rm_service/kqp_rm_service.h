#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_pattern_cache.h>

#include <ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <array>
#include <bitset>
#include <functional>


namespace NKikimr {
namespace NKqp {

namespace NRm {

/// memory pools
enum EKqpMemoryPool : ui32 {
    Unspecified = 0,
    ScanQuery   = 1, // slow allocations via ResourceBroker
    DataQuery   = 2, // fast allocations via memory-arena

    Count = 3
};

using TOnResourcesSnapshotCallback = std::function<void(TVector<NKikimrKqp::TKqpNodeResources>&&)>;

/// resources request
struct TKqpResourcesRequest {
    ui32 ExecutionUnits = 0;
    EKqpMemoryPool MemoryPool = EKqpMemoryPool::Unspecified;
    ui64 Memory = 0;

    TString ToString() const {
        return TStringBuilder() << "TKqpResourcesRequest{ MemoryPool: " << (ui32) MemoryPool << ", Memory: " << Memory
           << ", ExecutionUnits: " << ExecutionUnits << " }";
    }
};

/// detailed information on allocation failure
struct TKqpNotEnoughResources {
    std::bitset<32> State;

    bool NotReady() const         { return State.test(0); }
    bool ExecutionUnits() const   { return State.test(1); }
    bool QueryMemoryLimit() const { return State.test(2); }
    bool ScanQueryMemory() const  { return State.test(3); }
    bool DataQueryMemory() const  { return State.test(4); }

    void SetNotReady()         { State.set(0); }
    void SetExecutionUnits()   { State.set(1); }
    void SetQueryMemoryLimit() { State.set(2); }
    void SetScanQueryMemory()  { State.set(3); }
    void SetDataQueryMemory()  { State.set(4); }
};

/// local resources snapshot
struct TKqpLocalNodeResources {
    ui32 ExecutionUnits = 0;
    std::array<ui64, EKqpMemoryPool::Count> Memory;
};

/// per node singleton with instant API
class IKqpResourceManager : private TNonCopyable {
public:
    virtual ~IKqpResourceManager() = default;

    virtual bool AllocateResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources,
        TKqpNotEnoughResources* details = nullptr) = 0;

    using TResourcesAllocatedCallback = std::function<void(NActors::TActorSystem* as)>;
    using TNotEnoughtResourcesCallback = std::function<void(NActors::TActorSystem* as, const TString& reason, bool byTimeout)>;

    virtual bool AllocateResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources,
        TResourcesAllocatedCallback&& onSuccess, TNotEnoughtResourcesCallback&& onFail, TDuration timeout = {}) = 0;

    virtual void FreeResources(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) = 0;
    virtual void FreeResources(ui64 txId, ui64 taskId) = 0;
    virtual void FreeResources(ui64 txId) = 0;

    virtual void NotifyExternalResourcesAllocated(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) = 0;
    virtual void NotifyExternalResourcesFreed(ui64 txId, ui64 taskId, const TKqpResourcesRequest& resources) = 0;
    virtual void NotifyExternalResourcesFreed(ui64 txId, ui64 taskId) = 0;

    virtual void RequestClusterResourcesInfo(TOnResourcesSnapshotCallback&& callback) = 0;

    virtual TKqpLocalNodeResources GetLocalResources() const = 0;
    virtual NKikimrConfig::TTableServiceConfig::TResourceManager GetConfig() = 0;

    virtual std::shared_ptr<NMiniKQL::TComputationPatternLRUCache> GetPatternCache() = 0;

    virtual ui32 GetNodeId() {
        return 0;
    }
};


NActors::IActor* CreateTakeResourcesSnapshotActor(
    const TString& boardPath,
    std::function<void(TVector<NKikimrKqp::TKqpNodeResources>&&)>&& callback);


struct TResourceSnapshotState {
    std::shared_ptr<TVector<NKikimrKqp::TKqpNodeResources>> Snapshot;
    TMutex Lock;
};

struct TEvKqpResourceInfoExchanger {
    struct TEvPublishResource : public TEventLocal<TEvPublishResource,
        TKqpResourceInfoExchangerEvents::EvPublishResource>
    {
        const NKikimrKqp::TKqpNodeResources Resources;
        TEvPublishResource(NKikimrKqp::TKqpNodeResources resources) : Resources(std::move(resources)) {
        }
    };

    struct TEvSendResources : public TEventPB<TEvSendResources, NKikimrKqp::TResourceExchangeSnapshot,
        TKqpResourceInfoExchangerEvents::EvSendResources>
    {};
};

NActors::IActor* CreateKqpResourceInfoExchangerActor(TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<TResourceSnapshotState> resourceSnapshotState,
    const NKikimrConfig::TTableServiceConfig::TResourceManager::TInfoExchangerSettings& settings);

} // namespace NRm

struct TKqpProxySharedResources {
    std::atomic<ui32> AtomicLocalSessionCount{0};
};

NActors::IActor* CreateKqpResourceManagerActor(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
    TIntrusivePtr<TKqpCounters> counters, NActors::TActorId resourceBroker = {},
    std::shared_ptr<TKqpProxySharedResources> kqpProxySharedResources = nullptr);

std::shared_ptr<NRm::IKqpResourceManager> GetKqpResourceManager(TMaybe<ui32> nodeId = Nothing());
std::shared_ptr<NRm::IKqpResourceManager> TryGetKqpResourceManager(TMaybe<ui32> nodeId = Nothing());

} // namespace NKqp
} // namespace NKikimr
