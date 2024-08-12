#pragma once

#include "defs.h"

#include "console.h"
#include "console__scheme.h"
#include "tx_processor.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/location.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/mind/tenant_slot_broker_impl.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/set.h>

namespace NKikimr::NConsole {

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;
using NSchemeShard::TEvSchemeShard;
using NTenantSlotBroker::TEvTenantSlotBroker;
using NTenantSlotBroker::TSlotDescription;
using ::NMonitoring::TDynamicCounterPtr;

class TConsole;

class TTenantsManager : public TActorBootstrapped<TTenantsManager> {
private:
    using TBase = TActorBootstrapped<TTenantsManager>;

public:
    using TUnitsCount = THashMap<std::pair<TString, TString>, ui64>;
    using TSlotsCount = THashMap<TSlotDescription, ui64>;
    using TDomainId = TPathId;

    //////////////////////////////////////////////////
    // Counters
    //////////////////////////////////////////////////

    enum ECounter {
        // Simple counters.
        COUNTER_TENANTS,
        COUNTER_CREATE_REQUESTS,
        COUNTER_ALTER_REQUESTS,
        COUNTER_REMOVE_REQUESTS,
        COUNTER_STATUS_REQUESTS,
        COUNTER_LIST_REQUESTS,
        COUNTER_DESCRIBE_REQUESTS,
        COUNTER_GET_OPERATION_REQUESTS,
        COUNTER_ALLOC_POOL_FAILED,
        COUNTER_REMOVE_POOL_FAILED,
        COUNTER_CONFIGURE_SUBDOMAIN_FAILED,
        COUNTER_REMOVE_SUBDOMAIN_FAILED,
        COUNTER_COMPUTATIONAL_QUOTA_EXCEEDED,
        COUNTER_COMPUTATIONAL_LOAD_QUOTA_EXCEEDED,
        COUNTER_TENANTS_QUOTA_EXCEEDED,

        COUNTER_COUNT_SIMPLE,
        // Counters supplied with kind.
        COUNTER_REQUESTED_STORAGE_UNITS = COUNTER_COUNT_SIMPLE,
        COUNTER_ALLOCATED_STORAGE_UNITS,
        COUNTER_REGISTERED_UNITS,

        // Counters supplied with kind and zone.
        COUNTER_COMPUTATIONAL_UNITS,

        // Counters supplied with status.
        COUNTER_CREATE_RESPONSES,
        COUNTER_ALTER_RESPONSES,
        COUNTER_REMOVE_RESPONSES,
        COUNTER_STATUS_RESPONSES,
        COUNTER_GET_OPERATION_RESPONSES,

        COUNTER_COUNT
    };

    class TCounters {
    private:
        static const std::array<TString, COUNTER_COUNT> SensorNames;
        static const THashSet<ECounter> DerivSensors;

    public:
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

        TCounters(TDynamicCounterPtr counters)
            : Counters(counters)
        {
            for (ui64 i = 0; i < COUNTER_COUNT_SIMPLE; ++i)
                SimpleCounters[i] = counters->GetCounter(SensorName(static_cast<ECounter>(i)),
                                                         IsDeriv(static_cast<ECounter>(i)));
        }

        static const TString &SensorName(ECounter counter)
        {
            return SensorNames[counter];
        }

        static bool IsDeriv(ECounter counter)
        {
            return DerivSensors.contains(counter);
        }

        TCounterPtr GetCounter(ECounter counter)
        {
            return SimpleCounters[counter];
        }

        TCounterPtr GetCounter(const TString &kind,
                               ECounter counter)
        {
            auto key = std::make_pair(kind, counter);
            auto it = KindCounters.find(key);
            if (it != KindCounters.end())
                return it->second;

            auto group = Counters->GetSubgroup("kind", key.first);
            auto res = group->GetCounter(SensorName(counter), IsDeriv(counter));
            KindCounters[key] = res;

            return res;
        }

        TCounterPtr GetCounter(const TString &kind,
                               const TString &zone,
                               ECounter counter)
        {
            auto key = std::make_tuple(kind, zone, counter);
            auto it = KindZoneCounters.find(key);
            if (it != KindZoneCounters.end())
                return it->second;

            auto group1 = Counters->GetSubgroup("kind", kind);
            auto group2 = group1->GetSubgroup("zone", zone);
            auto res = group2->GetCounter(SensorName(counter), IsDeriv(counter));
            KindZoneCounters[key] = res;

            return res;
        }

        TCounterPtr GetCounter(Ydb::StatusIds::StatusCode code,
                               ECounter counter)
        {
            auto key = std::make_pair(code, counter);
            auto it = StatusCounters.find(key);
            if (it != StatusCounters.end())
                return it->second;

            auto group = Counters->GetSubgroup("status", ToString(key.first));
            auto res = group->GetCounter(SensorName(counter), IsDeriv(counter));
            StatusCounters[key] = res;

            return res;
        }

        void Inc(ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(counter)->Add(delta);
        }

        void Dec(ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(counter)->Sub(delta);
        }

        ui64 Get(ECounter counter)
        {
            return GetCounter(counter)->Val();
        }

        void Set(ECounter counter,
                 ui64 val)
        {
            *GetCounter(counter) = val;
        }

        void Inc(const TString &kind,
                 ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(kind, counter)->Add(delta);
        }

        void Dec(const TString &kind,
                 ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(kind, counter)->Sub(delta);
        }

        ui64 Get(const TString &kind,
                 ECounter counter)
        {
            return GetCounter(kind, counter)->Val();
        }

        void Set(const TString &kind,
                 ECounter counter,
                 ui64 val)
        {
            *GetCounter(kind, counter) = val;
        }

        void Inc(const TString &kind,
                 const TString &zone,
                 ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(kind, zone, counter)->Add(delta);
        }

        void Dec(const TString &kind,
                 const TString &zone,
                 ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(kind, zone, counter)->Sub(delta);
        }

        ui64 Get(const TString &kind,
                 const TString &zone,
                 ECounter counter)
        {
            return GetCounter(kind, zone, counter)->Val();
        }

        void Set(const TString &kind,
                 const TString &zone,
                 ECounter counter,
                 ui64 val)
        {
            *GetCounter(kind, zone, counter) = val;
        }

        void Inc(Ydb::StatusIds::StatusCode code,
                 ECounter counter,
                 ui64 delta = 1)
        {
            GetCounter(code, counter)->Add(delta);
        }

        void AddUnits(const TUnitsCount &units);
        void RemoveUnits(const TUnitsCount &units);

    private:
        TDynamicCounterPtr Counters;
        std::array<TCounterPtr, COUNTER_COUNT_SIMPLE> SimpleCounters;
        THashMap<std::pair<TString, ECounter>, TCounterPtr> KindCounters;
        THashMap<std::tuple<TString, TString, ECounter>, TCounterPtr> KindZoneCounters;
        THashMap<std::pair<Ydb::StatusIds::StatusCode, ECounter>, TCounterPtr> StatusCounters;
    };

    //////////////////////////////////////////////////
    // State
    //////////////////////////////////////////////////

    struct TStoragePool : public TThrRefBase {
        using TPtr = TIntrusivePtr<TStoragePool>;

        enum EState {
            NOT_ALLOCATED,
            NOT_UPDATED,
            ALLOCATED,
            DELETED,
        };

        TStoragePool(
                const TString &kind,
                const NKikimrBlobStorage::TDefineStoragePool &config,
                bool borrowed)
            : Kind(kind)
            , Config(config)
            , AllocatedNumGroups(0)
            , State(NOT_ALLOCATED)
            , Borrowed(borrowed)
        {
            Config.SetKind(kind);
        }

        TStoragePool(
                const TString &name,
                ui64 size,
                const TString &kind,
                const NKikimrBlobStorage::TDefineStoragePool &config,
                bool borrowed)
            : TStoragePool(kind, config, borrowed)
        {
            Config.SetName(name);
            Config.SetNumGroups(size);
        }

        TStoragePool(const TStoragePool &other) = default;

        void AddRequiredGroups(ui64 count)
        {
            Config.SetNumGroups(Config.GetNumGroups() + count);
        }

        void RevertRequiredGroups()
        {
            Config.SetNumGroups(AllocatedNumGroups);
        }

        ui64 GetGroups() const
        {
            return Config.GetNumGroups();
        }

        void SetScopeId(ui64 schemeShardId,
                        ui64 pathId)
        {
            SetScopeId(Config, schemeShardId, pathId);
        }

        void SetScopeId(const TDomainId& domainId)
        {
            SetScopeId(Config, domainId.OwnerId, domainId.LocalPathId);
        }

        static void SetScopeId(NKikimrBlobStorage::TDefineStoragePool &config,
                               ui64 schemeShardId,
                               ui64 pathId)
        {
            config.MutableScopeId()->SetX1(schemeShardId);
            config.MutableScopeId()->SetX2(pathId);
        }

        TString Kind;
        NKikimrBlobStorage::TDefineStoragePool Config;
        ui64 AllocatedNumGroups;
        EState State;
        bool Borrowed;
        TString Issue;
        TActorId Worker;
        size_t GroupFitErrors = 0;
    };

    struct TTenantSlotKind {
        TString Kind;
        TString TenantSlotType;
        TSet<TString> AllowedZones;

        bool operator==(const NKikimr::NConsole::TTenantsManager::TTenantSlotKind &rhs) const
        {
            return (Kind == rhs.Kind
                    && TenantSlotType == rhs.TenantSlotType
                    && AllowedZones == rhs.AllowedZones);
        }

        bool operator!=(const NKikimr::NConsole::TTenantsManager::TTenantSlotKind &rhs)
        {
            return !(*this == rhs);
        }
    };

    struct TAvailabilityZone {
        TString Name;
        TString DataCenter;
        bool ForceLocation;
        ui32 CollocationGroup;
        bool ForceCollocation;

        TAvailabilityZone()
            : ForceLocation(true)
            , CollocationGroup(0)
            , ForceCollocation(false)
        {
        }

        TAvailabilityZone(const TAvailabilityZone &other) = default;
        TAvailabilityZone(TAvailabilityZone &&other) = default;

        TAvailabilityZone(const TString &name,
                          const TString &dc,
                          bool forceLocation = true,
                          ui32 collocationGroup = 0,
                          bool forceCollocation = false)
            : Name(name)
            , DataCenter(dc)
            , ForceLocation(forceLocation)
            , CollocationGroup(collocationGroup)
            , ForceCollocation(forceCollocation)
        {
        }

        TAvailabilityZone &operator=(const TAvailabilityZone &other) = default;
        TAvailabilityZone &operator=(TAvailabilityZone &&other) = default;

        bool operator==(const NKikimr::NConsole::TTenantsManager::TAvailabilityZone &rhs) const
        {
            return (Name == rhs.Name
                    && DataCenter == rhs.DataCenter
                    && ForceLocation == rhs.ForceLocation
                    && CollocationGroup == rhs.CollocationGroup
                    && ForceCollocation == rhs.ForceCollocation);
        }

        bool operator!=(const NKikimr::NConsole::TTenantsManager::TAvailabilityZone &rhs) const
        {
            return !(*this == rhs);
        }
    };

    struct TTenantsConfig {
        TTenantsConfig();

        void Clear();
        void Parse(const NKikimrConsole::TTenantsConfig &config);
        bool Parse(const NKikimrConsole::TTenantsConfig &config, TString &error);
        TSlotDescription GetSlotKey(const TString &kind, const TString &zone) const;
        TSlotDescription GetSlotKey(const std::pair<TString, TString> &unit) const;

        void ParseComputationalUnits(const TUnitsCount &units,
                                     TSlotsCount &slots) const;

        ui64 DefaultStorageUnitsQuota;
        ui64 DefaultComputationalUnitsQuota;
        THashMap<TString, TAvailabilityZone> AvailabilityZones;
        THashMap<TString, TTenantSlotKind> TenantSlotKinds;
        ui64 TotalComputationalUnitsQuota;
        ui64 TotalComputationalUnitsLoadQuota;
        ui64 TenantsQuota;
    };

    struct TAllocatedComputationalUnit {
        TString Host;
        ui32 Port;
        TString Kind;
    };

    class TTenant : public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TTenant>;

        enum EState {
            CREATING_POOLS,
            CREATING_SUBDOMAIN,
            RUNNING,
            REMOVING_UNITS,
            REMOVING_SUBDOMAIN,
            REMOVING_POOLS,
            CONFIGURING_SUBDOMAIN,
        };

        enum EAction {
            CREATE = 1,
            REMOVE,
        };

        TTenant(const TString &path,
                EState state,
                const TString &token);

        static bool IsConfiguringState(EState state);
        static bool IsCreatingState(EState state);
        static bool IsRemovingState(EState state);
        static bool IsRunningState(EState state);

        bool IsConfiguring() const;
        bool IsCreating() const;
        bool IsRemoving() const;
        bool IsRunning() const;
        bool HasPoolsToCreate() const;
        bool HasPoolsToDelete() const;
        bool HasSubDomainKey() const;
        TString MakeStoragePoolName(const TString &poolTypeName);
        bool CheckComputationalUnitsQuota(const TUnitsCount &units,
                                          Ydb::StatusIds::StatusCode &code,
                                          TString &error);
        bool CheckStorageUnitsQuota(Ydb::StatusIds::StatusCode &code, TString &error, ui64 additionalUnits = 0);
        bool CheckQuota(Ydb::StatusIds::StatusCode &code, TString &error);
        void ParseComputationalUnits(const TTenantsConfig &config);
        void RemoveComputationalUnits();

        TString Path;
        EState State;
        ui64 Coordinators;
        ui64 Mediators;
        ui64 PlanResolution;
        ui32 TimeCastBucketsPerMediator;
        TActorId Worker;
        // <type> -> pool
        THashMap<TString, TStoragePool::TPtr> StoragePools;
        // <kind, az> -> count
        TUnitsCount ComputationalUnits;
        // <host, port> -> TAllocatedComputationalUnit
        THashMap<std::pair<TString, ui32>, TAllocatedComputationalUnit> RegisteredComputationalUnits;
        // <type, dc> -> count
        TSlotsCount Slots;
        bool SlotsAllocationConfirmed;
        TVector<TAutoPtr<IEventHandle>> StatusRequests;
        ui64 StorageUnitsQuota;
        ui64 ComputationalUnitsQuota;
        TVector<TActorId> Subscribers;
        // Error code and issue are used to report the last
        // problem with tenant.
        Ydb::StatusIds::StatusCode ErrorCode;
        TString Issue;
        ui64 TxId;
        NACLib::TUserToken UserToken;
        // Subdomain version is incremented on each pool creation.
        ui64 SubdomainVersion;
        // Last subdomain version configured in SchemeShard.
        ui64 ConfirmedSubdomain;
        // Attributes to attach to subdomain.
        NKikimrSchemeOp::TAlterUserAttributes Attributes;
        // Current generation.
        ui64 Generation;
        // Subdomain ID.
        TDomainId DomainId;
        TDomainId SharedDomainId;
        bool IsExternalSubdomain;
        bool IsExternalHive;
        bool IsExternalSysViewProcessor;
        bool IsExternalStatisticsAggregator;
        bool IsExternalBackupController;
        bool IsGraphShardEnabled = false;
        bool AreResourcesShared;
        THashSet<TTenant::TPtr> HostedTenants;

        TMaybe<Ydb::Cms::SchemaOperationQuotas> SchemaOperationQuotas;
        TMaybe<Ydb::Cms::DatabaseQuotas> DatabaseQuotas;
        TString CreateIdempotencyKey;
        TString AlterIdempotencyKey;
    };

    struct TRemovedTenant {
        TString Path;
        TString Issue;
        ui64 TxId;
        // Status code for removal operation.
        Ydb::StatusIds::StatusCode Code;
        // Error code copied from tenant state.
        Ydb::StatusIds::StatusCode ErrorCode;
        // Idempotency key used in a creation request
        TString CreateIdempotencyKey;
    };

    struct TSlotStats {
        struct TSlotCounters {
            ui64 Allocated = 0;
            ui64 Connected = 0;
            ui64 Free = 0;
        };

        THashMap<TString, TSlotCounters> SlotsByType;
        TSlotCounters Total;

        TSlotStats() = default;

        void Clear();

        void AllocateSlots(const TSlotsCount &slots);
        void DeallocateSlots(const TSlotsCount &slots);

        void UpdateStats(const NKikimrTenantSlotBroker::TSlotStats &stats);
    };

public:
    //////////////////////////////////////////////////
    // PRIVATE EVENTS
    //////////////////////////////////////////////////

    struct TEvPrivate {
        enum EEv {
            EvStateLoaded = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSubdomainFailed,
            EvSubdomainCreated,
            EvSubdomainKey,
            EvSubdomainReady,
            EvSubdomainRemoved,
            EvRetryAllocateResources,
            EvPoolAllocated,
            EvPoolFailed,
            EvPoolDeleted,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvStateLoaded : public TEventLocal<TEvStateLoaded, EvStateLoaded> {};

        struct TEvSubdomainFailed : public TEventLocal<TEvSubdomainFailed, EvSubdomainFailed> {
            TEvSubdomainFailed(TTenant::TPtr tenant,
                               const TString &issue,
                               Ydb::StatusIds::StatusCode code = Ydb::StatusIds::GENERIC_ERROR)
                : Tenant(tenant)
                , Issue(issue)
                , Code(code)
            {
            }

            TTenant::TPtr Tenant;
            TString Issue;
            Ydb::StatusIds::StatusCode Code;
        };

        struct TEvSubdomainCreated : public TEventLocal<TEvSubdomainCreated, EvSubdomainCreated> {
            TEvSubdomainCreated(TTenant::TPtr tenant, ui64 schemeShardId, ui64 pathId)
                : Tenant(tenant)
                , SchemeShardId(schemeShardId)
                , PathId(pathId)
            {
            }

            TTenant::TPtr Tenant;
            ui64 SchemeShardId;
            ui64 PathId;
        };

        struct TEvSubdomainKey : public TEventLocal<TEvSubdomainKey, EvSubdomainKey> {
            TEvSubdomainKey(TTenant::TPtr tenant, ui64 schemeShardId, ui64 pathId)
                : Tenant(tenant)
                , SchemeShardId(schemeShardId)
                , PathId(pathId)
            {
            }

            TTenant::TPtr Tenant;
            ui64 SchemeShardId;
            ui64 PathId;
        };

        struct TEvSubdomainReady : public TEventLocal<TEvSubdomainReady, EvSubdomainReady> {
        TEvSubdomainReady(TTenant::TPtr tenant,
                          ui64 version)
                : Tenant(tenant)
                , Version(version)
            {
            }

            TTenant::TPtr Tenant;
            ui64 Version;
        };

        struct TEvSubdomainRemoved : public TEventLocal<TEvSubdomainRemoved, EvSubdomainRemoved> {
            TEvSubdomainRemoved(TTenant::TPtr tenant)
                : Tenant(tenant)
            {
            }

            TTenant::TPtr Tenant;
        };

        struct TEvRetryAllocateResources : public TEventLocal<TEvRetryAllocateResources, EvRetryAllocateResources> {
            TEvRetryAllocateResources(const TString &name)
                : TenantName(name)
            {
            }

            TString TenantName;
        };

        struct TEvPoolAllocated : public TEventLocal<TEvPoolAllocated, EvPoolAllocated> {
            TEvPoolAllocated(TTenant::TPtr tenant, TStoragePool::TPtr pool)
                : Tenant(tenant)
                , Pool(pool)
            {
            }

            TTenant::TPtr Tenant;
            TStoragePool::TPtr Pool;
        };

        struct TEvPoolFailed : public TEventLocal<TEvPoolFailed, EvPoolFailed> {
            TEvPoolFailed(TTenant::TPtr tenant, TStoragePool::TPtr pool, const TString &issue)
                : Tenant(tenant)
                , Pool(pool)
                , Issue(issue)
            {
            }

            TTenant::TPtr Tenant;
            TStoragePool::TPtr Pool;
            TString Issue;
        };

        struct TEvPoolDeleted : public TEventLocal<TEvPoolDeleted, EvPoolDeleted> {
            TEvPoolDeleted(TTenant::TPtr tenant, TStoragePool::TPtr pool)
                : Tenant(tenant)
                , Pool(pool)
            {
            }

            TTenant::TPtr Tenant;
            TStoragePool::TPtr Pool;
        };
    };

public:
    class TTxAlterTenant;
    class TTxCreateTenant;
    class TTxRemoveComputationalUnits;
    class TTxRemoveTenant;
    class TTxRemoveTenantDone;
    class TTxRemoveTenantFailed;
    class TTxUpdateConfirmedSubdomain;
    class TTxUpdatePoolState;
    class TTxRevertPoolState;
    class TTxUpdateSubDomainKey;
    class TTxUpdateTenantState;
    class TTxUpdateTenantPoolConfig;

    ITransaction *CreateTxAlterTenant(TEvConsole::TEvAlterTenantRequest::TPtr &ev);
    ITransaction *CreateTxCreateTenant(TEvConsole::TEvCreateTenantRequest::TPtr &ev);
    ITransaction *CreateTxRemoveComputationalUnits(TTenant::TPtr tenant);
    ITransaction *CreateTxRemoveTenant(TEvConsole::TEvRemoveTenantRequest::TPtr &ev);
    ITransaction *CreateTxRemoveTenantDone(TTenant::TPtr tenant);
    ITransaction *CreateTxRemoveTenantFailed(TTenant::TPtr tenant,
                                             Ydb::StatusIds::StatusCode code);
    ITransaction *CreateTxUpdateConfirmedSubdomain(const TString &name,
                                                   ui64 version,
                                                   TActorId worker);
    ITransaction *CreateTxUpdateTenantState(const TString &name,
                                            TTenant::EState state,
                                            TActorId worker = TActorId());
    ITransaction *CreateTxUpdateSubDomainKey(const TString &path,
                                             ui64 schemeShardId,
                                             ui64 pathId,
                                             TActorId worker);
    ITransaction *CreateTxUpdatePoolState(TTenant::TPtr tenant,
                                          TStoragePool::TPtr pool,
                                          TActorId worker,
                                          TStoragePool::EState state);
    ITransaction *CreateTxRevertPoolState(TTenant::TPtr tenant,
                                          TStoragePool::TPtr pool,
                                          TActorId worker);
    ITransaction *CreateTxUpdateTenantPoolConfig(TEvConsole::TEvUpdateTenantPoolConfig::TPtr &ev);

    void ClearState();
    void SetConfig(const NKikimrConsole::TTenantsConfig &config);

    TTenant::TPtr FindZoneKindUsage(const TString &kind);
    TTenant::TPtr FindComputationalUnitKindUsage(const TString &kind);
    TTenant::TPtr FindComputationalUnitKindUsage(const TString &kind, const TString &zone);

    TTenant::TPtr GetTenant(const TString &name);
    TTenant::TPtr GetTenant(const TDomainId &domainId);
    void AddTenant(TTenant::TPtr tenant);
    void RemoveTenant(TTenant::TPtr tenant);
    void RemoveTenantFailed(TTenant::TPtr tenant,
                            Ydb::StatusIds::StatusCode code);
    void ChangeTenantState(TTenant::TPtr tenant,
                           TTenant::EState state,
                           const TActorContext &ctx);
    bool CheckTenantSlots(TTenant::TPtr tenant, const NKikimrTenantSlotBroker::TTenantState &state);
    bool HasEnoughPoolIds(ui64 count);
    bool MakeBasicPoolCheck(const TString &type, ui64 count, Ydb::StatusIds::StatusCode &code, TString &error);
    bool MakeBasicComputationalUnitCheck(const TString &kind, const TString &zone,
                                         Ydb::StatusIds::StatusCode &code,
                                         TString &error);
    // Check we have enough quota to replace computational units
    // of specified tenant with new ones.
    bool CheckComputationalUnitsQuota(const TUnitsCount &units,
                                      TTenant::TPtr tenant,
                                      Ydb::StatusIds::StatusCode &code,
                                      TString &error);
    // Check if we have enough quota to allocate specified computational units.
    bool CheckComputationalUnitsQuota(const TUnitsCount &units,
                                      Ydb::StatusIds::StatusCode &code,
                                      TString &error);
    bool CheckTenantsConfig(const NKikimrConsole::TTenantsConfig &config,
                            Ydb::StatusIds::StatusCode &code,
                            TString &error);
    bool CheckAccess(const TString &token,
                     Ydb::StatusIds::StatusCode &code,
                     TString &error,
                     const TActorContext &ctx);
    Ydb::TOperationId MakeOperationId(const TString &path, ui64 txId, TTenant::EAction action);
    Ydb::TOperationId MakeOperationId(TTenant::TPtr tenant, TTenant::EAction action);
    TStoragePool::TPtr MakeStoragePool(TTenant::TPtr tenant, const TString &kind, ui64 size);

    void CreateSubDomain(TTenant::TPtr tenant, const TActorContext &ctx);
    void OpenSchemeShardPipe(const TActorContext &ctx);
    void OnSchemeShardPipeDestroyed(const TActorContext &ctx);
    void OpenTenantSlotBrokerPipe(const TActorContext &ctx);
    void OnTenantSlotBrokerPipeDestroyed(const TActorContext &ctx);
    void AllocateTenantPools(TTenant::TPtr tenant, const TActorContext &ctx);
    void DeleteTenantPools(TTenant::TPtr tenant, const TActorContext &ctx);
    void RequestTenantResources(TTenant::TPtr tenant, const TActorContext &ctx);
    void RequestTenantSlotsState(TTenant::TPtr tenant, const TActorContext &ctx);
    void RequestTenantSlotsStats(const TActorContext &ctx);
    void RetryResourcesRequests(const TActorContext &ctx);

    void FillTenantStatus(TTenant::TPtr tenant, Ydb::Cms::GetDatabaseStatusResult &status);
    void FillTenantAllocatedSlots(TTenant::TPtr tenant, Ydb::Cms::GetDatabaseStatusResult &status,
                                  const NKikimrTenantSlotBroker::TTenantState &slots);
    void CheckSubDomainKey(TTenant::TPtr tenant, const TActorContext &ctx);
    void ConfigureTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx);
    void CreateTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx);
    void DeleteTenantSubDomain(TTenant::TPtr tenant, const TActorContext &ctx);
    void ProcessTenantActions(TTenant::TPtr tenant, const TActorContext &ctx);
    TTenant::TPtr FillOperationStatus(const TString &id, Ydb::Operations::Operation &operation);
    void SendTenantNotifications(TTenant::TPtr tenant, TTenant::EAction action,
                                 Ydb::StatusIds::StatusCode code, const TActorContext &ctx);
    void DumpStateHTML(IOutputStream &os);

    void ProcessOrDelayTx(ITransaction *tx,
                          const TActorContext &ctx);

    void DbAddTenant(TTenant::TPtr tenant,
                     TTransactionContext &txc,
                     const TActorContext &ctx);
    bool DbLoadState(TTransactionContext &txc,
                     const TActorContext &ctx);
    void DbRemoveComputationalUnit(TTenant::TPtr tenant,
                                   const TString &kind,
                                   const TString &zone,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx);
    void DbRemoveComputationalUnits(TTenant::TPtr tenant,
                                    TTransactionContext &txc,
                                    const TActorContext &ctx);
    void DbRemoveRegisteredUnit(TTenant::TPtr tenant,
                                const TString &host,
                                ui32 port,
                                TTransactionContext &txc,
                                const TActorContext &ctx);
    void DbRemoveTenantAndPools(TTenant::TPtr tenant,
                                TTransactionContext &txc,
                                const TActorContext &ctx);
    void DbUpdateComputationalUnit(TTenant::TPtr tenant,
                                   const TString &kind,
                                   const TString &zone,
                                   ui64 count,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx);
    void DbUpdateConfirmedSubdomain(TTenant::TPtr tenant,
                                    ui64 version,
                                    TTransactionContext &txc,
                                    const TActorContext &ctx);
    void DbUpdatePool(TTenant::TPtr tenant,
                      TStoragePool::TPtr pool,
                      TTransactionContext &txc,
                      const TActorContext &ctx);
    void DbUpdatePoolConfig(TTenant::TPtr tenant,
                            TStoragePool::TPtr pool,
                            const NKikimrBlobStorage::TDefineStoragePool &config,
                            TTransactionContext &txc,
                            const TActorContext &ctx);
    void DbUpdatePoolState(TTenant::TPtr tenant,
                           TStoragePool::TPtr pool,
                           TStoragePool::EState state,
                           TTransactionContext &txc,
                           const TActorContext &ctx);
    void DbUpdatePoolState(TTenant::TPtr tenant,
                           TStoragePool::TPtr pool,
                           TStoragePool::EState state,
                           ui32 allocatedNumGroups,
                           TTransactionContext &txc,
                           const TActorContext &ctx);
    void DbUpdateRegisteredUnit(TTenant::TPtr tenant,
                                const TString &host,
                                ui32 port,
                                const TString &kind,
                                TTransactionContext &txc,
                                const TActorContext &ctx);
    void DbUpdateRemovedTenant(TTenant::TPtr tenant,
                               Ydb::StatusIds::StatusCode code,
                               TTransactionContext &txc,
                               const TActorContext &ctx);
    void DbUpdateTenantAlterIdempotencyKey(TTenant::TPtr tenant,
                                           const TString &idempotencyKey,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx);
    void DbUpdateTenantUserAttributes(TTenant::TPtr tenant,
                                      const NKikimrSchemeOp::TAlterUserAttributes &attributes,
                                      TTransactionContext &txc,
                                      const TActorContext &ctx);
    void DbUpdateTenantGeneration(TTenant::TPtr tenant,
                                  ui64 generation,
                                  TTransactionContext &txc,
                                  const TActorContext &ctx);
    void DbUpdateTenantState(TTenant::TPtr tenant,
                             TTenant::EState state,
                             TTransactionContext &txc,
                             const TActorContext &ctx);
    void DbUpdateTenantSubdomain(TTenant::TPtr tenant,
                                 ui64 schemeShardId,
                                 ui64 pathId,
                                 TTransactionContext &txc,
                                 const TActorContext &ctx);
    void DbUpdateTenantUserToken(TTenant::TPtr tenant,
                                 const TString &userToken,
                                 TTransactionContext &txc,
                                 const TActorContext &ctx);
    void DbUpdateSubdomainVersion(TTenant::TPtr tenant,
                                  ui64 version,
                                  TTransactionContext &txc,
                                  const TActorContext &ctx);
    void DbUpdateSchemaOperationQuotas(TTenant::TPtr tenant,
                                       const Ydb::Cms::SchemaOperationQuotas &quotas,
                                       TTransactionContext &txc,
                                       const TActorContext &ctx);
    void DbUpdateDatabaseQuotas(TTenant::TPtr tenant,
                                const Ydb::Cms::DatabaseQuotas &quotas,
                                TTransactionContext &txc,
                                const TActorContext &ctx);

    void Handle(TEvConsole::TEvAlterTenantRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvCreateTenantRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvDescribeTenantOptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetOperationRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetTenantStatusRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvListTenantsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvNotifyOperationCompletionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvRemoveTenantRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvUpdateTenantPoolConfig::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPoolAllocated::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPoolDeleted::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPoolFailed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvRetryAllocateResources::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvStateLoaded::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubdomainFailed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubdomainCreated::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubdomainKey::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubdomainReady::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubdomainRemoved::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvSlotStats::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvTenantState::TPtr &ev, const TActorContext &ctx);

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::CMS_TENANTS);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvConsole::TEvAlterTenantRequest, Handle);
            HFuncTraced(TEvConsole::TEvCreateTenantRequest, Handle);
            HFuncTraced(TEvConsole::TEvDescribeTenantOptionsRequest, Handle);
            HFuncTraced(TEvConsole::TEvGetOperationRequest, Handle);
            HFuncTraced(TEvConsole::TEvGetTenantStatusRequest, Handle);
            HFuncTraced(TEvConsole::TEvListTenantsRequest, Handle);
            HFuncTraced(TEvConsole::TEvNotifyOperationCompletionRequest, Handle);
            HFuncTraced(TEvConsole::TEvRemoveTenantRequest, Handle);
            HFuncTraced(TEvConsole::TEvUpdateTenantPoolConfig, Handle);
            HFuncTraced(TEvPrivate::TEvStateLoaded, Handle);
            HFuncTraced(TEvPrivate::TEvPoolAllocated, Handle);
            HFuncTraced(TEvPrivate::TEvPoolDeleted, Handle);
            HFuncTraced(TEvPrivate::TEvPoolFailed, Handle);
            HFuncTraced(TEvPrivate::TEvRetryAllocateResources, Handle);
            HFuncTraced(TEvPrivate::TEvSubdomainFailed, Handle);
            HFuncTraced(TEvPrivate::TEvSubdomainCreated, Handle);
            HFuncTraced(TEvPrivate::TEvSubdomainKey, Handle);
            HFuncTraced(TEvPrivate::TEvSubdomainReady, Handle);
            HFuncTraced(TEvPrivate::TEvSubdomainRemoved, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvSlotStats, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvTenantState, Handle);

        default:
            Y_ABORT("TTenantsManager::StateWork unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

public:
    TTenantsManager(TConsole &self,
                    TDomainsInfo::TDomain::TPtr domain,
                    TDynamicCounterPtr counters,
                    NKikimrConfig::TFeatureFlags featureFlags)
        : Self(self)
        , Domain(domain)
        , Counters(counters)
        , FeatureFlags(std::move(featureFlags))
    {
    }

    ~TTenantsManager()
    {
        ClearState();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_TENANTS_MANAGER;
    }

    void Bootstrap(const TActorContext &ctx);
    void Detach();

private:
    TConsole &Self;
    TDomainsInfo::TDomain::TPtr Domain;
    TActorId TenantSlotBrokerPipe;
    THashMap<TString, TTenant::TPtr> Tenants;
    THashMap<TDomainId, TString> TenantIdToName;
    THashMap<TString, TRemovedTenant> RemovedTenants;
    TTenantsConfig Config;
    TTxProcessor::TPtr TxProcessor;
    TQueue<THolder<ITransaction>> DelayedTxs;
    TSlotStats SlotStats;
    TCounters Counters;
    NKikimrConfig::TFeatureFlags FeatureFlags;
};

} // namespace NKikimr::NConsole

template<>
inline void Out<NKikimr::NConsole::TTenantsManager::TTenant::EState>(IOutputStream& o, NKikimr::NConsole::TTenantsManager::TTenant::EState x) {
    if (x == NKikimr::NConsole::TTenantsManager::TTenant::CREATING_POOLS)
        o << "CREATING_POOLS";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::CREATING_SUBDOMAIN)
        o << "CREATING_SUBDOMAIN";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::RUNNING)
        o << "RUNNING";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::REMOVING_UNITS)
        o << "REMOVING_UNITS";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::REMOVING_SUBDOMAIN)
        o << "REMOVING_SUBDOMAIN";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::REMOVING_POOLS)
        o << "REMOVING_POOLS";
    else if (x == NKikimr::NConsole::TTenantsManager::TTenant::CONFIGURING_SUBDOMAIN)
        o << "CONFIGURING_SUBDOMAIN";
    else
        o << "<UNKNOWN>";
    return;
}

template<>
inline void Out<NKikimr::NConsole::TTenantsManager::TStoragePool::EState>(IOutputStream& o, NKikimr::NConsole::TTenantsManager::TStoragePool::EState x) {
    if (x == NKikimr::NConsole::TTenantsManager::TStoragePool::NOT_ALLOCATED)
        o << "NOT_ALLOCATED";
    else if (x == NKikimr::NConsole::TTenantsManager::TStoragePool::NOT_UPDATED)
        o << "NOT_UPDATED";
    else if (x == NKikimr::NConsole::TTenantsManager::TStoragePool::ALLOCATED)
        o << "ALLOCATED";
    else if (x == NKikimr::NConsole::TTenantsManager::TStoragePool::DELETED)
        o << "DELETED";
    else
        o << "<UNKNOWN>";
    return;
}
