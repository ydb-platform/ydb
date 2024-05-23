#include "tenant_runtime.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/labels_maintainer.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceproxy/sequenceproxy.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/persqueue/pq.h>
#include <ydb/core/statistics/aggregator/aggregator.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/dirut.h>

namespace NKikimr {

using namespace NTabletFlatExecutor;
using namespace NSchemeShard;
using namespace NConsole;
using namespace NTenantSlotBroker;

const ui64 SCHEME_SHARD1_ID = MakeTabletID(false, 0x0000000000840100);
const ui64 SCHEME_SHARD2_ID = MakeTabletID(false, 0x0000000000840101);
const ui64 HIVE_ID = MakeTabletID(false, 0x0000000000840102);

const TString DOMAIN1_NAME = "dc-1";
const TString TENANT1_1_NAME = "/dc-1/users/tenant-1";
const TString TENANT1_2_NAME = "/dc-1/users/tenant-2";
const TString TENANT1_3_NAME = "/dc-1/users/tenant-3";
const TString TENANT1_4_NAME = "/dc-1/users/tenant-4";
const TString TENANT1_5_NAME = "/dc-1/users/tenant-5";
const TString TENANT2_1_NAME = "/dc-2/users/tenant-1";
const TString TENANT2_2_NAME = "/dc-2/users/tenant-2";
const TString TENANT2_3_NAME = "/dc-2/users/tenant-3";
const TString TENANT2_4_NAME = "/dc-2/users/tenant-4";
const TString TENANT2_5_NAME = "/dc-2/users/tenant-5";
const TString TENANT1_U_NAME = "/dc-1/users/tenant-unknown";
const TString TENANTU_1_NAME = "/dc-3/users/tenant-1";
const TString DOMAIN1_SLOT1 = "dc-1/slot-1";
const TString DOMAIN1_SLOT2 = "dc-1/slot-2";
const TString DOMAIN1_SLOT3 = "dc-1/slot-3";
const TString STATIC_SLOT = "static-slot";
const TString SLOT1_TYPE = "small";
const TString SLOT2_TYPE = "medium";
const TString SLOT3_TYPE = "large";

const TSubDomainKey DOMAIN1_KEY = {SCHEME_SHARD1_ID, 1};

const TSubDomainKey TENANT1_1_KEY = {SCHEME_SHARD1_ID, 101};
const TSubDomainKey TENANT1_2_KEY = {SCHEME_SHARD1_ID, 102};
const TSubDomainKey TENANT1_3_KEY = {SCHEME_SHARD1_ID, 103};
const TSubDomainKey TENANT1_4_KEY = {SCHEME_SHARD1_ID, 104};
const TSubDomainKey TENANT1_5_KEY = {SCHEME_SHARD1_ID, 105};
const TSubDomainKey TENANT2_1_KEY = {SCHEME_SHARD2_ID, 201};
const TSubDomainKey TENANT2_2_KEY = {SCHEME_SHARD2_ID, 202};
const TSubDomainKey TENANT2_3_KEY = {SCHEME_SHARD2_ID, 203};
const TSubDomainKey TENANT2_4_KEY = {SCHEME_SHARD2_ID, 204};
const TSubDomainKey TENANT2_5_KEY = {SCHEME_SHARD2_ID, 205};

const TString ZONE1 = "zone1";
const TString ZONE2 = "zone2";
const TString ZONE3 = "zone3";
const TString ZONE_ANY = "any";

const TTenantTestConfig DefaultTenantTestConfig = {
    // Domains {name, schemeshard {{ subdomain_names }}}
    {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
    // HiveId
    HIVE_ID,
    // FakeTenantSlotBroker
    true,
    // FakeSchemeShard
    true,
    // CreateConsole
    false,
    // Nodes
    {{
            // Node0
            {
                // TenantPoolConfig
                {
                    // Static slots {tenant, {cpu, memory, network}}
                    {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                    "node-type"
                }
            }
    }},
    // DataCenterCount
    1
};

namespace {

class TFakeNodeWhiteboardService : public TActorBootstrapped<TFakeNodeWhiteboardService> {
public:
    void Bootstrap(const TActorContext &ctx)
    {
        Y_UNUSED(ctx);
        Become(&TFakeNodeWhiteboardService::StateWork);
    }

    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

class TFakeSchemeShard : public TActor<TFakeSchemeShard>, public TTabletExecutedFlat {
    void Handle(TEvSchemeShard::TEvDescribeScheme::TPtr &ev, const TActorContext &ctx)
    {
        if (HoldResolve) {
            Queue.push_back(ev.Release());
            ctx.Send(Sender, new TEvents::TEvWakeup());
            return;
        }

        auto &rec = ev->Get()->Record;
        TString path = rec.GetPath();
        if (!path)
            path = Paths[rec.GetPathId()];

        auto *resp = new TEvSchemeShard::TEvDescribeSchemeResultBuilder;
        resp->Record.SetPath(path);

        auto it = SubDomains.find(path);
        if (it != SubDomains.end()) {
            resp->Record.SetStatus(NKikimrScheme::StatusSuccess);
            auto &self = *resp->Record.MutablePathDescription()->MutableSelf();
            self.SetName(ev->Get()->Record.GetPath());
            self.SetPathId(it->second);
            self.SetSchemeshardId(TabletID());
            self.SetPathType(NKikimrSchemeOp::EPathTypeSubDomain);
            auto &domain = *resp->Record.MutablePathDescription()->MutableDomainDescription();
            domain.SetSchemeShardId_Depricated(TabletID());
            domain.SetPathId_Depricated(it->second);
            domain.MutableDomainKey()->SetSchemeShard(TabletID());
            domain.MutableDomainKey()->SetPathId(it->second);
        } else {
            resp->Record.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
        }
        ctx.Send(ev->Sender, resp);
    }

    void Handle(TEvTest::TEvHoldResolve::TPtr &ev, const TActorContext &ctx)
    {
        if (ev->Get()->Hold) {
            HoldResolve = true;
            ctx.Send(Sender, new TEvents::TEvWakeup());
        } else {
            HoldResolve = false;
            for (auto &e : Queue)
                StateWork(e);
            Queue.clear();
        }
    }

    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FLAT_SCHEMESHARD_ACTOR;
    }

    TFakeSchemeShard(const TActorId &tablet, TTabletStorageInfo *info,
                     TActorId sender, const TVector<std::pair<TString, ui64>> &subDomains)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Sender(sender)
        , HoldResolve(false)
    {
        for (auto &subDomain : subDomains) {
            SubDomains.emplace(subDomain.first, subDomain.second);
            Paths.emplace(subDomain.second, subDomain.first);
        }
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvDescribeScheme, Handle);
            HFunc(TEvTest::TEvHoldResolve, Handle);
        default:
            break;
        }
    }

    THashMap<TString, ui64> SubDomains;
    THashMap<ui64, TString> Paths;
    TActorId Sender;
    TVector<TAutoPtr<IEventHandle>> Queue;
    bool HoldResolve;
};

class TFakeBSController : public TActor<TFakeBSController>, public TTabletExecutedFlat {
    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

public:
    TFakeBSController(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
    {
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

class TFakeTenantSlotBroker : public TActor<TFakeTenantSlotBroker>, public TTabletExecutedFlat {
    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void SendState(const TString &name, TActorId sender, const TActorContext &ctx)
    {
        auto *resp = new TEvTenantSlotBroker::TEvTenantState;
        resp->Record.SetTenantName(name);
        if (State.contains(name))
            resp->Record.MutableRequiredSlots()->CopyFrom(State.at(name).GetRequiredSlots());
        ctx.Send(sender, resp);
    }

    void Handle(TEvTenantSlotBroker::TEvAlterTenant::TPtr &ev, const TActorContext &ctx)
    {
        State[ev->Get()->Record.GetTenantName()] = ev->Get()->Record;
        SendState(ev->Get()->Record.GetTenantName(), ev->Sender, ctx);
    }

    void Handle(TEvTenantSlotBroker::TEvGetTenantState::TPtr &ev, const TActorContext &ctx)
    {
        SendState(ev->Get()->Record.GetTenantName(), ev->Sender, ctx);
    }

public:
    TFakeTenantSlotBroker(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
    {
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTenantSlotBroker::TEvAlterTenant, Handle);
            HFunc(TEvTenantSlotBroker::TEvGetTenantState, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
        }
    }
private:
    THashMap<TString, NKikimrTenantSlotBroker::TAlterTenant> State;
};

class TFakeHive : public TActor<TFakeHive>, public TTabletExecutedFlat {
    struct TClientInfo {
        TString Name;
        TSubDomainKey Key;
        TEvLocal::TEvStatus::EStatus Status = TEvLocal::TEvStatus::StatusDead;
        NKikimrTabletBase::TMetrics ResourceLimit;

        bool operator==(const TClientInfo &other) {
            return Name == other.Name
                && Status == other.Status
                && ResourceLimit.GetCPU() == other.ResourceLimit.GetCPU()
                && ResourceLimit.GetMemory() == other.ResourceLimit.GetMemory()
                && ResourceLimit.GetNetwork() == other.ResourceLimit.GetNetwork();
        }

        bool operator!=(const TClientInfo &other) {
            return !(*this == other);
        }
    };

    void ResolveKey(TSubDomainKey key, const TActorContext &ctx)
    {
        TActorId clientId = ctx.Register(NKikimr::NTabletPipe::CreateClient(ctx.SelfID, key.GetSchemeShard()));
        auto *request = new TEvSchemeShard::TEvDescribeScheme(key.GetSchemeShard(), key.GetPathId());
        NTabletPipe::SendData(ctx, clientId, request);
        ctx.Send(clientId, new NKikimr::TEvTabletPipe::TEvShutdown);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx)
    {
        Clients.erase(ev->Sender);
        CheckState(ctx);
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx)
    {
        const auto &rec = ev->Get()->GetRecord();
        UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus(), NKikimrScheme::StatusSuccess);
        auto &path = rec.GetPath();
        auto shardId = rec.GetPathDescription().GetSelf().GetSchemeshardId();
        auto pathId = rec.GetPathDescription().GetSelf().GetPathId();
        SubDomainKeys[TSubDomainKey(shardId, pathId)] = path;
        CheckState(ctx);
    }

    TActorId Boot(const TActorContext& ctx, TTabletTypes::EType tabletType, std::function<IActor* (const TActorId &, TTabletStorageInfo *)> op,
                  TBlobStorageGroupType::EErasureSpecies erasure)
    {
        TIntrusivePtr<TBootstrapperInfo> bi(new TBootstrapperInfo(new TTabletSetupInfo(op, TMailboxType::Simple, 0,
                                                                                       TMailboxType::Simple, 0)));
        return ctx.ExecutorThread.RegisterActor(CreateBootstrapper(CreateTestTabletInfo(State.NextTabletId, tabletType, erasure), bi.Get()));
    }

    void SendDeletionNotification(ui64 tabletId, TActorId waiter, const TActorContext& ctx)
    {
        TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();
        FillTabletInfo(response->Record, tabletId, nullptr);
        ctx.Send(waiter, response.Release());
    }

    void FillTabletInfo(NKikimrHive::TEvResponseHiveInfo& response, ui64 tabletId, const TFakeHiveTabletInfo *info)
    {
        auto& tabletInfo = *response.AddTablets();
        tabletInfo.SetTabletID(tabletId);
        if (info) {
            tabletInfo.SetTabletType(info->Type);
            tabletInfo.SetState(200);
        }
    }

    bool MaybeCreateTablet(TEvHive::TEvCreateTablet::TPtr &ev, const TActorContext &ctx)
    {
        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        const std::pair<ui64, ui64> key(ev->Get()->Record.GetOwner(), ev->Get()->Record.GetOwnerIdx());
        const auto type = ev->Get()->Record.GetTabletType();
        const auto bootMode = ev->Get()->Record.GetTabletBootMode();
        auto it = State.Tablets.find(key);
        TActorId bootstrapperActorId;
        if (it == State.Tablets.end()) {
            if (ev->Get()->Record.AllowedDomainsSize()) {
                bool found = false;
                for (auto &key : ev->Get()->Record.GetAllowedDomains()) {
                    for (auto &pr : Clients) {
                        if (pr.second.Key == TSubDomainKey(key)) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found)
                    return false;
            }

            if (bootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL) {
            } else if (type == TTabletTypes::Coordinator) {
                bootstrapperActorId = Boot(ctx, type, &CreateFlatTxCoordinator, DataGroupErasure);
            } else if (type == TTabletTypes::Mediator) {
                bootstrapperActorId = Boot(ctx, type, &CreateTxMediator, DataGroupErasure);
            } else if (type == TTabletTypes::SchemeShard) {
                bootstrapperActorId = Boot(ctx, type, &CreateFlatTxSchemeShard, DataGroupErasure);
            } else if (type == TTabletTypes::Hive) {
                bootstrapperActorId = Boot(ctx, type, &CreateDefaultHive, DataGroupErasure);
            } else if (type == TTabletTypes::SysViewProcessor) {
                bootstrapperActorId = Boot(ctx, type, &NSysView::CreateSysViewProcessor, DataGroupErasure);
            } else if (type == TTabletTypes::SequenceShard) {
                bootstrapperActorId = Boot(ctx, type, &NSequenceShard::CreateSequenceShard, DataGroupErasure);
            } else if (type == TTabletTypes::ReplicationController) {
                bootstrapperActorId = Boot(ctx, type, &NReplication::CreateController, DataGroupErasure);
            } else if (type == TTabletTypes::PersQueue) {
                bootstrapperActorId = Boot(ctx, type, &NKikimr::CreatePersQueue, DataGroupErasure);
            } else if (type == TTabletTypes::StatisticsAggregator) {
                bootstrapperActorId = Boot(ctx, type, &NStat::CreateStatisticsAggregator, DataGroupErasure);
            } else {
                status = NKikimrProto::ERROR;
            }

            if (status == NKikimrProto::OK) {
                ui64 tabletId = State.NextTabletId;
                it = State.Tablets.insert(std::make_pair(key, TFakeHiveTabletInfo(type, tabletId, bootstrapperActorId))).first;
                State.TabletIdToOwner[tabletId] = key;
                ++State.NextTabletId;
            }
        } else {
            if (it->second.Type != type) {
                status = NKikimrProto::ERROR;
            }
        }

        auto response = new TEvHive::TEvCreateTabletReply(status, key.first,
                                                          key.second, it->second.TabletId,
                                                          TabletID());
        ctx.Send(ev->Sender, response, 0, ev->Cookie);
        return true;
    }

    void Handle(TEvHive::TEvCreateTablet::TPtr &ev, const TActorContext &ctx)
    {
        if (!MaybeCreateTablet(ev, ctx)) {
            PostponedTablets.push_back(std::move(ev));
        }
    }

    void Handle(TEvHive::TEvDeleteTablet::TPtr &ev, const TActorContext &ctx) {
        NKikimrHive::TEvDeleteTablet& rec = ev->Get()->Record;
        TVector<ui64> localIds;
        for (size_t i = 0; i < rec.ShardLocalIdxSize(); ++i) {
            localIds.push_back(rec.GetShardLocalIdx(i));
            auto it = State.Tablets.find(std::make_pair<ui64, ui64>(rec.GetShardOwnerId(), rec.GetShardLocalIdx(i)));
            if (it != State.Tablets.end()) {
                ctx.Send(ctx.SelfID, new TEvFakeHive::TEvNotifyTabletDeleted(it->second.TabletId));

                TActorId bootstrapperActorId = it->second.BootstrapperActorId;
                ctx.Send(bootstrapperActorId, new TEvBootstrapper::TEvStandBy());

                for (TActorId waiter : it->second.DeletionWaiters) {
                    SendDeletionNotification(it->second.TabletId, waiter, ctx);
                }
                State.TabletIdToOwner.erase(it->second.TabletId);
                State.Tablets.erase(it);
            }
        }
        ctx.Send(ev->Sender, new TEvHive::TEvDeleteTabletReply(NKikimrProto::OK, TabletID(), rec.GetTxId_Deprecated(), rec.GetShardOwnerId(), localIds));
    }

    void Handle(TEvHive::TEvDeleteOwnerTablets::TPtr &ev, const TActorContext &ctx) {
        NKikimrHive::TEvDeleteOwnerTablets& rec = ev->Get()->Record;
        TVector<ui64> toDelete;
        for (auto item : State.Tablets) {
            if (item.first.first != rec.GetOwner()) {
                continue;
            }
            toDelete.push_back(item.first.second);
        }

        for (auto idx: toDelete) {
            auto it = State.Tablets.find(std::pair<ui64, ui64>(rec.GetOwner(), idx));
            if (it != State.Tablets.end()) {
                ctx.Send(ctx.SelfID, new TEvFakeHive::TEvNotifyTabletDeleted(it->second.TabletId));

                TActorId bootstrapperActorId = it->second.BootstrapperActorId;
                ctx.Send(bootstrapperActorId, new TEvBootstrapper::TEvStandBy());

                for (TActorId waiter : it->second.DeletionWaiters) {
                    SendDeletionNotification(it->second.TabletId, waiter, ctx);
                }
                State.TabletIdToOwner.erase(it->second.TabletId);
                State.Tablets.erase(it);
            }
        }

        ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::OK, TabletID(), rec.GetOwner(), rec.GetTxId()));
    }

    void Handle(TEvLocal::TEvRegisterNode::TPtr &ev, const TActorContext &ctx)
    {
        auto &record = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.ServicedDomainsSize(), 1);

        TClientInfo info;
        info.Key = TSubDomainKey(record.GetServicedDomains(0));
        Clients.emplace(ev->Sender, info);

        auto *ping = new TEvLocal::TEvPing;
        ping->Record.SetHiveId(HiveId);
        ping->Record.SetHiveGeneration(1);
        ctx.Send(ev->Sender, ping, IEventHandle::FlagTrackDelivery);

        if (!SubDomainKeys.contains(info.Key))
            ResolveKey(info.Key, ctx);
    }

    void Handle(TEvLocal::TEvStatus::TPtr &ev, const TActorContext &ctx)
    {
        auto &record = ev->Get()->Record;

        UNIT_ASSERT(Clients.contains(ev->Sender));
        if (record.GetStatus() == TEvLocal::TEvStatus::StatusDead) {
            Clients.erase(ev->Sender);
        } else {
            auto &client = Clients[ev->Sender];
            client.Status = static_cast<TEvLocal::TEvStatus::EStatus>(record.GetStatus());
            client.ResourceLimit = record.GetResourceMaximum();

            for (auto it = PostponedTablets.begin(); it != PostponedTablets.end(); ) {
                if (MaybeCreateTablet(*it, ctx)) {
                    it = PostponedTablets.erase(it);
                } else {
                    ++it;
                }
            }
        }
        CheckState(ctx);
    }

    void Handle(TEvTest::TEvWaitHiveState::TPtr &ev, const TActorContext &ctx)
    {
        UNIT_ASSERT(!WaitForState);
        for (auto &state : ev->Get()->States) {
            if (state.ResourceLimit.GetCPU()
                || state.ResourceLimit.GetMemory()
                || state.ResourceLimit.GetNetwork()) {
                TClientInfo info;
                info.Name = state.TenantName;
                info.Status = TEvLocal::TEvStatus::StatusOk;
                info.ResourceLimit = state.ResourceLimit;
                ExpectedState.emplace(std::make_pair(info.Name, info));
            }
        }
        WaitForState = true;
        CheckState(ctx);
    }

    void CheckState(const TActorContext &ctx)
    {
        if (!WaitForState)
            return;

        ui64 missing = 0;
        THashMap<TString, TClientInfo> tenants;
        for (auto &pr : Clients) {
            if (!SubDomainKeys.contains(pr.second.Key)) {
                return;
            }
            auto name = SubDomainKeys.at(pr.second.Key);
            if (tenants.contains(name)) {
                auto &tenant = tenants[name];
                if (pr.second.Status != TEvLocal::TEvStatus::StatusOk)
                    tenant.Status = pr.second.Status;
                tenant.ResourceLimit.SetCPU(tenant.ResourceLimit.GetCPU() + pr.second.ResourceLimit.GetCPU());
                tenant.ResourceLimit.SetMemory(tenant.ResourceLimit.GetMemory() + pr.second.ResourceLimit.GetMemory());
                tenant.ResourceLimit.SetNetwork(tenant.ResourceLimit.GetNetwork() + pr.second.ResourceLimit.GetNetwork());
            } else {
                tenants[name] = pr.second;
                tenants[name].Name = name;
            }
        }
        for (auto &pr : tenants) {
            if (pr.second.Status != TEvLocal::TEvStatus::StatusOk) {
                ++missing;
            } else if (!ExpectedState.contains(pr.first)) {
                return;
            } else if (ExpectedState[pr.first] != pr.second) {
                return;
            }
        }

        if (ExpectedState.size() + missing != tenants.size()) {
            return;
        }

        WaitForState = false;
        ExpectedState.clear();
        ctx.Send(Sender, new TEvTest::TEvHiveStateHit);
    }

    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_ACTOR;
    }

    TFakeHive(const TActorId &tablet, TTabletStorageInfo *info, TActorId sender,
              ui64 hiveId, const THashMap<TSubDomainKey, TString> &subDomainKeys)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Sender(sender)
        , WaitForState(false)
        , HiveId(hiveId)
        , SubDomainKeys(subDomainKeys)
    {
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::HIVE);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            HFuncTraced(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFuncTraced(TEvHive::TEvCreateTablet, Handle);
            HFuncTraced(TEvHive::TEvDeleteTablet, Handle);
            HFuncTraced(TEvHive::TEvDeleteOwnerTablets, Handle);
            HFuncTraced(TEvLocal::TEvRegisterNode, Handle);
            HFuncTraced(TEvLocal::TEvStatus, Handle);
            HFuncTraced(TEvTest::TEvWaitHiveState, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            IgnoreFunc(TEvLocal::TEvSyncTablets);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                //Y_ABORT("TFakeHive::StateWork unexpected event type: %" PRIx32 " event: %s",
                //       ev->GetTypeRewrite(), ev->HasEvent() ? ~ev->GetBase()->ToString() : "serialized?");
            }
        }
    }

    THashMap<TActorId, TClientInfo> Clients;
    TActorId Sender;
    THashMap<TString, TClientInfo> ExpectedState;
    bool WaitForState;
    ui64 HiveId;
    THashMap<TSubDomainKey, TString> SubDomainKeys;
    TFakeHiveState State;
    TList<TEvHive::TEvCreateTablet::TPtr> PostponedTablets;
};

} // anonymous namespace

bool IsTabletActiveEvent(IEventHandle& ev)
{
    if (ev.GetTypeRewrite() == NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
        if (ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>()->Record.GetState()
            == NKikimrWhiteboard::TTabletStateInfo::Active) {
            return true;
        }
    }
    return false;
}

struct TWaitTenantSlotBrokerInitialization {
    TWaitTenantSlotBrokerInitialization(ui32 poolCount)
        : PoolCount(poolCount)
        , NodeInfoCount(0)
    {
        Y_ABORT_UNLESS(PoolCount);
    }

    bool operator()(IEventHandle &ev) {
        if (ev.GetTypeRewrite() == TEvTenantPool::EvTenantPoolStatus) {
            if (PoolCount) {
                --PoolCount;
                ++NodeInfoCount;
            }
        } else if (ev.GetTypeRewrite() == TEvInterconnect::EvNodeInfo) {
            if (NodeInfoCount) {
                --NodeInfoCount;
                if (!PoolCount && !NodeInfoCount)
                    return true;
            }
        }

        return false;
    }

    ui32 PoolCount;
    ui32 NodeInfoCount;
};

void TTenantTestRuntime::CreateTenantPool(ui32 nodeIndex, const TTenantTestConfig::TTenantPoolConfig &config)
{
    NKikimrTabletBase::TMetrics limit;
    TLocalConfig::TPtr localConfig = new TLocalConfig;
    localConfig->TabletClassInfo[TTabletTypes::Dummy].SetupInfo
        = new TTabletSetupInfo(&CreateFlatDummyTablet,
                               TMailboxType::Simple, 0,
                               TMailboxType::Simple, 0);

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(localConfig);
    for (auto &slot : config.StaticSlots) {
        NKikimrTenantPool::TSlotConfig slotConfig;
        slotConfig.SetId(STATIC_SLOT);
        slotConfig.SetTenantName(slot.Tenant);
        slotConfig.MutableResourceLimit()->SetCPU(slot.Limit.CPU);
        slotConfig.MutableResourceLimit()->SetMemory(slot.Limit.Memory);
        slotConfig.MutableResourceLimit()->SetNetwork(slot.Limit.Network);
        tenantPoolConfig->AddStaticSlot(slotConfig);
    }
    tenantPoolConfig->NodeType = config.NodeType;
    tenantPoolConfig->StaticSlotLabel = Extension.GetMonitoringConfig().GetDatabaseLabels()
        .GetStaticSlotLabelValue();

    TActorId actorId = Register(NKikimr::CreateTenantPool(tenantPoolConfig), nodeIndex, 0, TMailboxType::Revolving, 0);
    EnableScheduleForActor(actorId, true);
    RegisterService(MakeTenantPoolRootID(), actorId, nodeIndex);
}

void TTenantTestRuntime::CreateTenantPool(ui32 nodeIndex)
{
    CreateTenantPool(nodeIndex, Config.Nodes[nodeIndex].TenantPoolConfig);
}

void TTenantTestRuntime::Setup(bool createTenantPools)
{
    if (ENABLE_DETAILED_LOG) {
        SetLogPriority(NKikimrServices::LOCAL, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::TENANT_POOL, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::LABELS_MAINTAINER, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::TENANT_SLOT_BROKER, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::CMS, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::CMS_CONFIGS, NLog::PRI_TRACE);
        //SetLogPriority(NKikimrServices::CMS_TENANTS, NLog::PRI_TRACE);
        SetLogPriority(NKikimrServices::CONFIGS_DISPATCHER, NLog::PRI_TRACE);
        SetLogPriority(NKikimrServices::CONFIGS_CACHE, NLog::PRI_TRACE);
        SetLogPriority(NKikimrServices::HIVE, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);

        //SetLogPriority(NKikimrServices::TX_MEDIATOR, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::PIPE_CLIENT, NLog::PRI_DEBUG);
        //SetLogPriority(NKikimrServices::PIPE_SERVER, NLog::PRI_DEBUG);

        SetupMonitoring();
    }

    TAppPrepare app;

    app.FeatureFlags = Extension.GetFeatureFlags();
    app.ImmediateControlsConfig = Extension.GetImmediateControlsConfig();
    app.ClearDomainsAndHive();

    ui32 planResolution = 500;

    Y_ABORT_UNLESS(Config.Domains.size() == 1);

    // Add domains info.
    for (ui32 i = 0; i < Config.Domains.size(); ++i) {
        auto &domain = Config.Domains[i];
        NKikimrBlobStorage::TDefineStoragePool hddPool;
        hddPool.SetBoxId(1);
        hddPool.SetErasureSpecies("none");
        hddPool.SetVDiskKind("Default");
        hddPool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);
        TDomainsInfo::TDomain::TStoragePoolKinds poolTypes;
        poolTypes["hdd"] = hddPool;
        poolTypes["hdd-1"] = hddPool;
        poolTypes["hdd-2"] = hddPool;
        poolTypes["hdd-3"] = hddPool;
        auto domainPtr = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(domain.Name, i, domain.SchemeShardId,
                                                                planResolution,
                                                                TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
                                                                TVector<ui64>{TDomainsInfo::MakeTxMediatorIDFixed(1)},
                                                                TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
                                                                poolTypes);

        TVector<ui64> ids = GetTxAllocatorTabletIds();
        ids.insert(ids.end(), domainPtr->TxAllocators.begin(), domainPtr->TxAllocators.end());
        SetTxAllocatorTabletIds(ids);

        app.AddDomain(domainPtr.Release());
        app.AddHive(Config.HiveId);
    }

    app.InitIcb(Config.Nodes.size());

    for (size_t i = 0; i < Config.Nodes.size(); ++i) {
        AddLocalService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(GetNodeId(i)),
                        TActorSetupCmd(new TFakeNodeWhiteboardService, TMailboxType::Simple, 0), i);
    }

    SetupChannelProfiles(app);
    SetupBasicServices(*this, app);

    if (ENABLE_DETAILED_LOG) {
        Register(NTabletMonitoringProxy::CreateTabletMonitoringProxy());
    }

    if (const auto& domain = GetAppData().DomainsInfo->Domain) {
        for (auto id : domain->TxAllocators) {
            auto aid = CreateTestBootstrapper(*this, CreateTestTabletInfo(id, TTabletTypes::TxAllocator), &CreateTxAllocator);
            EnableScheduleForActor(aid, true);
        }
        for (auto id : domain->Coordinators) {
            auto aid = CreateTestBootstrapper(*this, CreateTestTabletInfo(id, TTabletTypes::Coordinator), &CreateFlatTxCoordinator);
            EnableScheduleForActor(aid, true);
        }
        for (auto id : domain->Mediators) {
            auto aid = CreateTestBootstrapper(*this, CreateTestTabletInfo(id, TTabletTypes::Mediator), &CreateTxMediator);
            EnableScheduleForActor(aid, true);
        }
    }

    // Create Scheme Shards
    Sender = AllocateEdgeActor();
    for (size_t i = 0; i < Config.Domains.size(); ++i) {
        auto &domain = Config.Domains[i];
        SubDomainKeys[TSubDomainKey(domain.SchemeShardId, 1)] = domain.Name;
        if (Config.FakeSchemeShard) {
            ui32 pathId = 100;
            TVector<std::pair<TString, ui64>> subdomains;
            for (auto &subDomain : domain.Subdomains) {
                TSubDomainKey key(domain.SchemeShardId, pathId++);
                SubDomainKeys[key] = subDomain;
                subdomains.push_back(std::make_pair(subDomain, key.GetPathId()));
            }

            auto info = CreateTestTabletInfo(domain.SchemeShardId, TTabletTypes::Dummy, TErasureType::ErasureNone);
            TActorId actorId = CreateTestBootstrapper(*this, info, [sender=Sender, subdomains](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                    return new TFakeSchemeShard(tablet, info, sender, subdomains);
                });
            EnableScheduleForActor(actorId, true);
        } else {
            auto info = CreateTestTabletInfo(domain.SchemeShardId, TTabletTypes::SchemeShard, TErasureType::ErasureNone);
            TActorId actorId = CreateTestBootstrapper(*this, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                    return CreateFlatTxSchemeShard(tablet, info);
                });
            EnableScheduleForActor(actorId, true);

            // Init scheme root.
            {
                auto evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(1, domain.SchemeShardId);
                auto transaction = evTx->Record.AddTransaction();
                transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain);
                transaction->SetWorkingDir("/");
                auto op = transaction->MutableSubDomain();
                op->SetName(domain.Name);

                for (const auto& [kind, pool] : GetAppData().DomainsInfo->GetDomain(0).StoragePoolTypes) {
                    auto* p = op->AddStoragePools();
                    p->SetKind(kind);
                    p->SetName(pool.GetName());
                }

                SendToPipe(domain.SchemeShardId, Sender, evTx.Release(), 0, GetPipeConfigWithRetries());

                {
                    TAutoPtr<IEventHandle> handle;
                    auto event = GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
                    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSchemeshardId(), domain.SchemeShardId);
                    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
                }

                auto evSubscribe = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(1);
                SendToPipe(domain.SchemeShardId, Sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

                {
                    TAutoPtr<IEventHandle> handle;
                    auto event = GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
                    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 1);
                }
            }
            Y_ABORT_UNLESS(domain.Subdomains.empty(), "Pre-initialized subdomains are not supported for real SchemeShard");
        }
    }

    // Create TxProxy services
    for (size_t i = 0; i< Config.Nodes.size(); ++i) {
        IActor* txProxy = CreateTxProxy(GetTxAllocatorTabletIds());
        TActorId txProxyId = Register(txProxy, i);
        RegisterService(MakeTxProxyID(), txProxyId, i);
    }

    // Create LongTx services
    for (size_t i = 0; i< Config.Nodes.size(); ++i) {
        IActor* longTxService = NLongTxService::CreateLongTxService();
        TActorId longTxServiceId = Register(longTxService, i);
        EnableScheduleForActor(longTxServiceId, true);
        RegisterService(NLongTxService::MakeLongTxServiceID(GetNodeId(i)), longTxServiceId, i);
    }

    // Create sequence proxies
    for (size_t i = 0; i< Config.Nodes.size(); ++i) {
        IActor* sequenceProxy = NSequenceProxy::CreateSequenceProxy();
        TActorId sequenceProxyId = Register(sequenceProxy, i);
        RegisterService(NSequenceProxy::MakeSequenceProxyServiceID(), sequenceProxyId, i);
    }

    // Create Hive.
    {
        auto info = CreateTestTabletInfo(Config.HiveId, TTabletTypes::Dummy, TErasureType::ErasureNone);
        TActorId actorId = CreateTestBootstrapper(*this, info, [this](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeHive(tablet, info, Sender, Config.HiveId, SubDomainKeys);
            });
        EnableScheduleForActor(actorId, true);
    }

    // Create BS Controller.
    {
        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        TActorId actorId = CreateTestBootstrapper(*this, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                //return new TFakeBSController(tablet, info);
                return CreateFlatBsController(tablet, info);
            });
        EnableScheduleForActor(actorId, true);

        // Get list of nodes to reveal IC ports.

        Send(new IEventHandle(GetNameserviceActorId(), Sender, new TEvInterconnect::TEvListNodes));
        TAutoPtr<IEventHandle> handle;
        auto reply1 = GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handle);

        NKikimrBlobStorage::TDefineHostConfig hostConfig;
        hostConfig.SetHostConfigId(1);
        hostConfig.AddDrive()->SetPath(TStringBuilder() << GetTempDir() << "pdisk_1.dat");
        NKikimrBlobStorage::TDefineBox boxConfig;
        boxConfig.SetBoxId(1);
        for (const TEvInterconnect::TNodeInfo &node : reply1->Nodes) {
            auto &host = *boxConfig.AddHost();
            host.SetHostConfigId(1);
            host.MutableKey()->SetFqdn(node.Host);
            host.MutableKey()->SetIcPort(node.Port);
        }
        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);
        request->Record.MutableRequest()->AddCommand()->MutableDefineBox()->CopyFrom(boxConfig);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        SendToPipe(MakeBSControllerID(), Sender, request.Release(), 0, pipeConfig);

        auto reply2 = GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply2->Record.GetResponse().GetSuccess(), true);
    }

    // Create Tenant Slot Pools
    Y_ABORT_UNLESS(GetNodeCount() >= Config.Nodes.size());
    TMultiSet<std::pair<TString, TEvLocal::TEvTenantStatus::EStatus>> statuses;

    if (createTenantPools) {
        for (size_t i = 0; i< Config.Nodes.size(); ++i) {
            auto &poolConfig = Config.Nodes[i].TenantPoolConfig;
            for (auto &slot : poolConfig.StaticSlots)
                statuses.insert(std::make_pair(CanonizePath(slot.Tenant), TEvLocal::TEvTenantStatus::STARTED));
            CreateTenantPool(i);
        }
    }

    // Create other local services
    for (size_t i = 0; i < Config.Nodes.size(); ++i) {
        if (Config.CreateConfigsDispatcher) {
            TMap<TString, TString> labels;
            for (const auto &label : Extension.GetLabels()) {
                labels[label.GetName()] = label.GetValue();
            }
            labels.emplace("node_id", ToString(i));
            auto aid = Register(CreateConfigsDispatcher(
                    NKikimr::NConfig::TConfigsDispatcherInitInfo {
                        .InitialConfig = Extension,
                        .Labels = labels,
                    }
                ));
            EnableScheduleForActor(aid, true);
            RegisterService(MakeConfigsDispatcherID(GetNodeId(0)), aid, 0);
        }

        Register(NKikimr::CreateLabelsMaintainer(Extension.GetMonitoringConfig()),
                 i, 0, TMailboxType::Revolving, 0);
    }

    // Wait until pools are up and domains are started.
    if (!statuses.empty()) {
        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TIsTenantStatus(statuses));
        DispatchEvents(options1);
    }

    // Create Console
    {
        auto info = CreateTestTabletInfo(MakeConsoleID(), TTabletTypes::Console, TErasureType::ErasureNone);
        TActorId actorId = CreateTestBootstrapper(*this, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return CreateConsole(tablet, info);
            });
        EnableScheduleForActor(actorId, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
        DispatchEvents(options);

        // Configure Console
        auto req = new NConsole::TEvConsole::TEvSetConfigRequest;
        auto &config = *req->Record.MutableConfig()->MutableTenantsConfig();
        auto zone1 = config.AddAvailabilityZoneKinds();
        zone1->SetKind(ZONE1);
        zone1->SetDataCenterName(ToString(1));
        auto zone2 = config.AddAvailabilityZoneKinds();
        zone2->SetKind(ZONE2);
        zone2->SetDataCenterName(ToString(2));
        auto zone3 = config.AddAvailabilityZoneKinds();
        zone3->SetKind(ZONE3);
        zone3->SetDataCenterName(ToString(3));
        auto zone4 = config.AddAvailabilityZoneKinds();
        zone4->SetKind(ZONE_ANY);
        zone4->SetDataCenterName(NTenantSlotBroker::ANY_DATA_CENTER);
        auto set1 = config.AddAvailabilityZoneSets();
        set1->SetName("all");
        set1->AddZoneKinds(ZONE1);
        set1->AddZoneKinds(ZONE2);
        set1->AddZoneKinds(ZONE3);
        set1->AddZoneKinds(ZONE_ANY);
        auto set2 = config.AddAvailabilityZoneSets();
        set2->SetName(ZONE1);
        set2->AddZoneKinds(ZONE1);
        auto set3 = config.AddAvailabilityZoneSets();
        set3->SetName(ZONE2);
        set3->AddZoneKinds(ZONE2);
        auto set4 = config.AddAvailabilityZoneSets();
        set4->SetName(ZONE3);
        set4->AddZoneKinds(ZONE3);
        auto unit1 = config.AddComputationalUnitKinds();
        unit1->SetKind(SLOT1_TYPE);
        unit1->SetTenantSlotType(SLOT1_TYPE);
        unit1->SetAvailabilityZoneSet(ZONE1);
        auto unit2 = config.AddComputationalUnitKinds();
        unit2->SetKind(SLOT2_TYPE);
        unit2->SetTenantSlotType(SLOT2_TYPE);
        unit2->SetAvailabilityZoneSet("all");
        auto unit3 = config.AddComputationalUnitKinds();
        unit3->SetKind(SLOT3_TYPE);
        unit3->SetTenantSlotType(SLOT3_TYPE);
        unit3->SetAvailabilityZoneSet("all");
        // Set configuration restrictions.
        auto &restrictions = *req->Record.MutableConfig()->MutableConfigsConfig()->MutableUsageScopeRestrictions();
        restrictions.AddAllowedNodeIdUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        restrictions.AddAllowedHostUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        restrictions.AddAllowedTenantUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        restrictions.AddAllowedTenantUsageScopeKinds(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
        restrictions.AddAllowedNodeTypeUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        SendToConsole(req);
        TAutoPtr<IEventHandle> handle;
        auto reply = GrabEdgeEventRethrow<TEvConsole::TEvSetConfigResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
    }

    // Create Tenant Slot Broker
    {
        auto info = CreateTestTabletInfo(MakeTenantSlotBrokerID(), TTabletTypes::TenantSlotBroker, TErasureType::ErasureNone);
        TActorId actorId = CreateTestBootstrapper(*this, info, [&config=this->Config](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                if (config.FakeTenantSlotBroker)
                    return new TFakeTenantSlotBroker(tablet, info);
                else
                    return NTenantSlotBroker::CreateTenantSlotBroker(tablet, info);
            });
        EnableScheduleForActor(actorId, true);

        // Wait until Tenant Slot Broker gets all statuses.
        if (createTenantPools && !Config.FakeTenantSlotBroker) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TWaitTenantSlotBrokerInitialization(Config.Nodes.size()));
            DispatchEvents(options);
        }
    }
}

TTenantTestRuntime::TTenantTestRuntime(const TTenantTestConfig &config,
                                       const NKikimrConfig::TAppConfig &extension,
                                       bool createTenantPools)
    : TTestBasicRuntime(config.Nodes.size(), config.DataCenterCount, false)
    , Config(config)
    , Extension(extension)
{
    Extension.MutableFeatureFlags()->SetEnableExternalHive(false);
    Setup(createTenantPools);
}

void TTenantTestRuntime::WaitForHiveState(const TVector<TEvTest::TEvWaitHiveState::TClientInfo> &state)
{
    TAutoPtr<IEventHandle> handle;
    SendToPipe(Config.HiveId, Sender, new TEvTest::TEvWaitHiveState(state));
    GrabEdgeEventRethrow<TEvTest::TEvHiveStateHit>(handle);
}

void TTenantTestRuntime::SendToBroker(IEventBase* event)
{
    SendToPipe(MakeTenantSlotBrokerID(), Sender, event, 0, GetPipeConfigWithRetries());
}

void TTenantTestRuntime::SendToConsole(IEventBase* event)
{
    SendToPipe(MakeConsoleID(), Sender, event, 0, GetPipeConfigWithRetries());
}

NKikimrTenantPool::TSlotStatus MakeSlotStatus(const TString &id, const TString &type, const TString &tenant,
                                              ui64 cpu, ui64 memory, ui64 network, const TString &label)
{
    NKikimrTenantPool::TSlotStatus res;
    res.SetId(id);
    res.SetType(type);
    res.SetAssignedTenant(tenant);
    res.SetLabel(label);
    res.MutableResourceLimit()->SetCPU(cpu);
    res.MutableResourceLimit()->SetMemory(memory);
    res.MutableResourceLimit()->SetNetwork(network);
    return res;
}

void CheckTenantPoolStatus(TTenantTestRuntime &runtime,
                           THashMap<TString, NKikimrTenantPool::TSlotStatus> status,
                           ui32 nodeId)
{
    runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(nodeId)),
                                  runtime.Sender,
                                  new TEvTenantPool::TEvGetStatus));
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvTenantPoolStatus>(handle);
    auto &rec = reply->Record;
    for (auto &slot : rec.GetSlots()) {
        UNIT_ASSERT(status.contains(slot.GetId()));
        auto &entry = status[slot.GetId()];
        UNIT_ASSERT_VALUES_EQUAL(slot.GetId(), entry.GetId());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetType(), entry.GetType());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetAssignedTenant(), entry.GetAssignedTenant());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetLabel(), entry.GetLabel());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetResourceLimit().GetCPU(), entry.GetResourceLimit().GetCPU());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetResourceLimit().GetMemory(), entry.GetResourceLimit().GetMemory());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetResourceLimit().GetNetwork(), entry.GetResourceLimit().GetNetwork());
        status.erase(slot.GetId());
    }
    UNIT_ASSERT(status.empty());
}

} // namespace NKikimr
