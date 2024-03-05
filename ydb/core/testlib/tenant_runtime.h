#pragma once
#include "defs.h"

#include <ydb/core/base/events.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/labels_maintainer.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <util/generic/string.h>

#ifndef NDEBUG
const bool ENABLE_DETAILED_LOG = true;
#else
const bool ENABLE_DETAILED_LOG = false;
#endif

namespace NKikimr {

struct TEvTest {
    enum EEv {
        EvHoldResolve = EventSpaceBegin(NKikimr::TKikimrEvents::ES_PRIVATE),
        EvWaitHiveState,
        EvHiveStateHit,
        EvActionSuccess,
        EvEnd
    };

    struct TEvHoldResolve : public TEventLocal<TEvHoldResolve, EvHoldResolve> {
        TEvHoldResolve(bool hold)
            : Hold(hold)
        {}

        bool Hold;
    };

    struct TEvWaitHiveState : public TEventLocal<TEvWaitHiveState, EvWaitHiveState> {
        struct TClientInfo {
            TClientInfo(const TString &name, ui64 cpu = 1, ui64 memory = 1, ui64 network = 1)
                : TenantName(name)
            {
                ResourceLimit.SetCPU(cpu);
                ResourceLimit.SetMemory(memory);
                ResourceLimit.SetNetwork(network);
            }

            TString TenantName;
            NKikimrTabletBase::TMetrics ResourceLimit;
        };

        TEvWaitHiveState(const TVector<TClientInfo> &states)
            : States(states)
        {}

        TVector<TClientInfo> States;
    };

    struct TEvHiveStateHit : public TEventLocal<TEvHiveStateHit, EvHiveStateHit> {};

    struct TEvActionSuccess : public TEventLocal<TEvActionSuccess, EvActionSuccess> {};
};

struct TTenantTestConfig {
    struct TDomainConfig {
        TString Name;
        ui64 SchemeShardId;
        TVector<TString> Subdomains;
    };

    struct TResourceLimit {
        ui64 CPU;
        ui64 Memory;
        ui64 Network;
    };

    struct TStaticSlotConfig {
        TString Tenant;
        TResourceLimit Limit;
    };

    struct TTenantPoolConfig {
        TVector<TStaticSlotConfig> StaticSlots;
        TString NodeType;
    };

    struct TNodeConfig {
        TTenantPoolConfig TenantPoolConfig;
    };

    TVector<TDomainConfig> Domains;
    ui64 HiveId;
    bool FakeTenantSlotBroker;
    bool FakeSchemeShard;
    bool CreateConsole;
    TVector<TNodeConfig> Nodes;
    ui32 DataCenterCount;
    bool CreateConfigsDispatcher = false;
};

extern const ui64 SCHEME_SHARD1_ID;
extern const ui64 HIVE_ID;
extern const ui64 SCHEME_SHARD2_ID;

extern const TString DOMAIN1_NAME;
extern const TString TENANT1_1_NAME;
extern const TString TENANT1_2_NAME;
extern const TString TENANT1_3_NAME;
extern const TString TENANT1_4_NAME;
extern const TString TENANT1_5_NAME;
extern const TString TENANT2_1_NAME;
extern const TString TENANT2_2_NAME;
extern const TString TENANT2_3_NAME;
extern const TString TENANT2_4_NAME;
extern const TString TENANT2_5_NAME;
extern const TString TENANT1_U_NAME;
extern const TString TENANTU_1_NAME;
extern const TString DOMAIN1_SLOT1;
extern const TString DOMAIN1_SLOT2;
extern const TString DOMAIN1_SLOT3;
extern const TString STATIC_SLOT;
extern const TString SLOT1_TYPE;
extern const TString SLOT2_TYPE;
extern const TString SLOT3_TYPE;

extern const TSubDomainKey DOMAIN1_KEY;

extern const TSubDomainKey TENANT1_1_KEY;
extern const TSubDomainKey TENANT1_2_KEY;
extern const TSubDomainKey TENANT1_3_KEY;
extern const TSubDomainKey TENANT1_4_KEY;
extern const TSubDomainKey TENANT1_5_KEY;
extern const TSubDomainKey TENANT2_1_KEY;
extern const TSubDomainKey TENANT2_2_KEY;
extern const TSubDomainKey TENANT2_3_KEY;
extern const TSubDomainKey TENANT2_4_KEY;
extern const TSubDomainKey TENANT2_5_KEY;

extern const TString ZONE1;
extern const TString ZONE2;
extern const TString ZONE3;
extern const TString ZONE_ANY;

struct TIsTenantStatus {
    TIsTenantStatus(const TString &tenant, TEvLocal::TEvTenantStatus::EStatus status)
    {
        Statuses.insert(std::make_pair(tenant, status));
    }

    template<typename ...Ts>
    TIsTenantStatus(const TString &tenant, TEvLocal::TEvTenantStatus::EStatus status, Ts... args)
        : TIsTenantStatus(args...)
    {
        Statuses.insert(std::make_pair(tenant, status));
    }

    TIsTenantStatus(TMultiSet<std::pair<TString, TEvLocal::TEvTenantStatus::EStatus>> statuses)
    {
        Statuses = std::move(statuses);
    }

    bool operator()(IEventHandle &ev) {
        if (ev.GetTypeRewrite() == TEvLocal::EvTenantStatus) {
            auto *e = ev.Get<TEvLocal::TEvTenantStatus>();
            auto it = Statuses.find(std::make_pair(e->TenantName, e->Status));
            if (it != Statuses.end()) {
                Statuses.erase(it);
                if (Statuses.empty())
                    return true;
            }
        }

        return false;
    }

    TMultiSet<std::pair<TString, TEvLocal::TEvTenantStatus::EStatus>> Statuses;
};

extern const TTenantTestConfig DefaultTenantTestConfig;

bool IsTabletActiveEvent(IEventHandle& ev);

class TTenantTestRuntime : public TTestBasicRuntime {
private:
    void Setup(bool createTenantPools = true);

public:
    TTenantTestRuntime(const TTenantTestConfig &config,
                       const NKikimrConfig::TAppConfig &extension = {},
                       bool createTenantPools = true);

    void CreateTenantPool(ui32 nodeIndex);
    void CreateTenantPool(ui32 nodeIndex, const TTenantTestConfig::TTenantPoolConfig &config);
    void WaitForHiveState(const TVector<TEvTest::TEvWaitHiveState::TClientInfo> &state);
    void SendToBroker(IEventBase* event);
    void SendToConsole(IEventBase* event);

    TActorId Sender;
    TTenantTestConfig Config;
    NKikimrConfig::TAppConfig Extension;
    THashMap<TSubDomainKey, TString> SubDomainKeys;
};

NKikimrTenantPool::TSlotStatus MakeSlotStatus(const TString &id, const TString &type, const TString &tenant,
                                              ui64 cpu, ui64 memory, ui64 network, const TString &label = "");
void CheckTenantPoolStatus(TTenantTestRuntime &runtime,
                           THashMap<TString, NKikimrTenantPool::TSlotStatus> status, ui32 nodeId = 0);

} // namespace NKikimr
