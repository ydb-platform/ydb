#include "local.h"
#include "tenant_pool.h"
#include "tenant_slot_broker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/queue.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace {

using namespace NTenantSlotBroker;
using namespace NConsole;

struct TTenantInfo;

struct TDynamicSlotInfo : public TThrRefBase {
    using TPtr = TIntrusivePtr<TDynamicSlotInfo>;

    TString Id;
    TString Type;
    TIntrusivePtr<TTenantInfo> AssignedTenant;
    NKikimrTabletBase::TMetrics ResourceLimit;
    TAutoPtr<IEventHandle> ActiveAction;
    TQueue<THolder<IEventHandle>> PendingActions;
    TString Label;
};

struct TTenantInfo : public TThrRefBase {
    using TPtr = TIntrusivePtr<TTenantInfo>;
    using EState = NKikimrTenantPool::EState;

    TTenantInfo(const TString &name)
        : Name(name)
        , HasStaticSlot(false)
        , LastStatus(TEvLocal::TEvTenantStatus::STOPPED)
        , State(EState::TENANT_ASSIGNED)
    {
    }

    void AddResourceLimit(const NKikimrTabletBase::TMetrics &limit)
    {
        if (ResourceLimit.HasCPU() && limit.HasCPU())
            ResourceLimit.SetCPU(ResourceLimit.GetCPU() + limit.GetCPU());
        else
            ResourceLimit.ClearCPU();

        if (ResourceLimit.HasMemory() && limit.HasMemory())
            ResourceLimit.SetMemory(ResourceLimit.GetMemory() + limit.GetMemory());
        else
            ResourceLimit.ClearMemory();

        if (ResourceLimit.HasNetwork() && limit.HasNetwork())
            ResourceLimit.SetNetwork(ResourceLimit.GetNetwork() + limit.GetNetwork());
        else
            ResourceLimit.ClearNetwork();
    }

    void ComputeResourceLimit()
    {
        ResourceLimit = StaticResourceLimit;

        auto it = AssignedSlots.begin();
        if (!HasStaticSlot && AssignedSlots.size()) {

            ResourceLimit = (*it)->ResourceLimit;
            ++it;
        }

        for (; it != AssignedSlots.end(); ++it)
            AddResourceLimit((*it)->ResourceLimit);
    }

    bool CompareLimit(const NKikimrTabletBase::TMetrics &limit)
    {
        if (ResourceLimit.GetCPU() != limit.GetCPU())
            return false;
        if (ResourceLimit.GetMemory() != limit.GetMemory())
            return false;
        if (ResourceLimit.GetNetwork() != limit.GetNetwork())
            return false;
        return true;
    }

    TString Name;
    THashSet<TDynamicSlotInfo::TPtr, TPtrHash> AssignedSlots;
    bool HasStaticSlot;
    NKikimrTabletBase::TMetrics StaticResourceLimit;
    NKikimrTabletBase::TMetrics ResourceLimit;
    TEvLocal::TEvTenantStatus::EStatus LastStatus;
    EState State;
    THashMap<TString, TString> Attributes;
    TSubDomainKey DomainKey;
};

struct TTenantSlotBrokerInfo {
    ui64 TabletId = 0;
    ui64 Generation = 0;
    ui64 SeqNo = 0;
    TActorId ActorId;
    TActorId Pipe;
};

class TDomainTenantPool : public TActorBootstrapped<TDomainTenantPool> {
    using TActorBase = TActorBootstrapped<TDomainTenantPool>;

    TString DomainName;
    TString LogPrefix;
    TActorId LocalID;
    TTenantSlotBrokerInfo TenantSlotBroker;
    TTenantPoolConfig::TPtr Config;
    THashMap<TString, TDynamicSlotInfo::TPtr> DynamicSlots;
    THashMap<TString, TTenantInfo::TPtr> Tenants;
    THashSet<TActorId> StatusSubscribers;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::TENANT_POOL_ACTOR;
    }

    TDomainTenantPool(const TString &domain, TActorId localID, TTenantPoolConfig::TPtr config)
        : DomainName(domain)
        , LocalID(localID)
        , Config(config)
    {
        LogPrefix = TStringBuilder() << "TDomainTenantPool(" << DomainName << ") ";
    }

    ~TDomainTenantPool()
    {
        for (auto &pr : Tenants)
            pr.second->AssignedSlots.clear();
        for (auto &pr : DynamicSlots)
            pr.second->AssignedTenant = nullptr;
    }

    void Die(const TActorContext &ctx) override
    {
        NTabletPipe::CloseAndForgetClient(SelfId(), TenantSlotBroker.Pipe);
        ctx.Send(LocalID, new TEvents::TEvPoisonPill);

        TActorBase::Die(ctx);
    }

    void TryToRegister(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(!TenantSlotBroker.Pipe);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::Seconds(1),
        };
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, TenantSlotBroker.TabletId, pipeConfig);
        TenantSlotBroker.Pipe = ctx.ExecutorThread.RegisterActor(pipe);

        auto request = MakeHolder<TEvTenantSlotBroker::TEvRegisterPool>();
        ActorIdToProto(TenantSlotBroker.Pipe, request->Record.MutableClientId());
        request->Record.SetSeqNo(++TenantSlotBroker.SeqNo);
        NTabletPipe::SendData(ctx, TenantSlotBroker.Pipe, request.Release());

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << "try to register in tenant slot broker (pipe "
                    << TenantSlotBroker.Pipe << ")");
    }

    void HandlePipeDestroyed(const TActorContext &ctx) {
        NTabletPipe::CloseAndForgetClient(SelfId(), TenantSlotBroker.Pipe);
        TryToRegister(ctx);
    }

    TTenantInfo::TPtr MaybeAddTenant(const TString &name)
    {
        auto &tenant = Tenants[name];
        if (!tenant)
            tenant = new TTenantInfo(name);
        return tenant;
    }

    void FillSlotStatus(TDynamicSlotInfo::TPtr slot, NKikimrTenantPool::TSlotStatus &status)
    {
        status.SetId(slot->Id);
        status.SetType(slot->Type);
        if (slot->AssignedTenant) {
            status.SetAssignedTenant(slot->AssignedTenant->Name);
            status.SetLabel(slot->Label);
            for (auto &pr : slot->AssignedTenant->Attributes) {
                auto &attr = *status.AddTenantAttributes();
                attr.SetKey(pr.first);
                attr.SetValue(pr.second);
            }
            status.SetState(slot->AssignedTenant->State);
            *status.MutableDomainKey() = NKikimrSubDomains::TDomainKey(slot->AssignedTenant->DomainKey);
        }
        status.MutableResourceLimit()->CopyFrom(slot->ResourceLimit);
    }

    void SendSlotStatus(TDynamicSlotInfo::TPtr slot, NKikimrTenantPool::EStatus status,
                        const TString &error, const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(slot->ActiveAction);
        auto event = MakeHolder<TEvTenantPool::TEvConfigureSlotResult>();
        event->Record.SetStatus(status);
        event->Record.SetError(error);
        FillSlotStatus(slot, *event->Record.MutableSlotStatus());
        ctx.Send(slot->ActiveAction->Sender, event.Release(), 0, slot->ActiveAction->Cookie);
        slot->ActiveAction.Reset();
    }

    void SendTenantStatus(TTenantInfo::TPtr tenant, NKikimrTenantPool::EStatus status,
                          const TString &error, const TActorContext &ctx)
    {
        for (auto &slot : tenant->AssignedSlots) {
            if (slot->ActiveAction) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "slot '" << slot->Id << "' configure for '"
                            << tenant->Name << "' finished with status " << status);

                SendSlotStatus(slot, status, error, ctx);
            }
        }
    }

    void SendConfigureError(TEvTenantPool::TEvConfigureSlot::TPtr &ev, const TString &slotId,
                            NKikimrTenantPool::EStatus status, const TString &error, const TActorContext &ctx)
    {
        auto event = MakeHolder<TEvTenantPool::TEvConfigureSlotResult>();
        event->Record.SetStatus(status);
        event->Record.SetError(error);
        event->Record.MutableSlotStatus()->SetId(slotId);
        ctx.Send(ev->Sender, event.Release(), 0, ev->Cookie);
    }

    void ProcessPendingActions(const THashSet<TDynamicSlotInfo::TPtr, TPtrHash> &slots, const TActorContext &ctx)
    {
        for (auto &slot : slots) {
            Y_ABORT_UNLESS(!slot->ActiveAction);
            if (slot->PendingActions) {
                slot->ActiveAction = std::move(slot->PendingActions.front());
                slot->PendingActions.pop();
                ProcessActiveAction(slot, ctx);
            }
        }
    }

    void SendTenantStatus(TTenantInfo::TPtr tenant, NKikimrTenantPool::EStatus status,
                          const TActorContext &ctx)
    {
        SendTenantStatus(tenant, status, "", ctx);
    }

    void SendStatusUpdates(const TActorContext &ctx)
    {
        for (auto &subscriber : StatusSubscribers) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "send status update to " << subscriber);
            ctx.Send(subscriber, BuildStatusEvent(true));
        }
    }

    void UpdateTenant(TTenantInfo::TPtr tenant, const TActorContext &ctx)
    {
        tenant->ComputeResourceLimit();
        if (tenant->LastStatus == TEvLocal::TEvTenantStatus::STOPPED
            || tenant->LastStatus == TEvLocal::TEvTenantStatus::UNKNOWN_TENANT) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "send request to add tenant " << tenant->Name
                        << " with resources " << tenant->ResourceLimit.ShortDebugString());

            auto event = MakeHolder<TEvLocal::TEvAddTenant>(tenant->Name,
                                                            tenant->ResourceLimit);
            ctx.Send(LocalID, event.Release());
        } else if (tenant->LastStatus == TEvLocal::TEvTenantStatus::STARTED) {
            if (tenant->HasStaticSlot || tenant->AssignedSlots) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "send request to alter tenant " << tenant->Name
                            << " with resources " << tenant->ResourceLimit.ShortDebugString());

                auto event = MakeHolder<TEvLocal::TEvAlterTenant>(tenant->Name,
                                                                  tenant->ResourceLimit);
                ctx.Send(LocalID, event.Release());
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "send request to remove tenant" << tenant->Name);

                auto event = MakeHolder<TEvLocal::TEvRemoveTenant>(tenant->Name);
                ctx.Send(LocalID, event.Release());
            }
        } else {
            Y_ABORT("unexpected tenant status");
        }
    }

    void AttachSlot(TDynamicSlotInfo::TPtr slot, TTenantInfo::TPtr tenant,
                    const TString &label, const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(!slot->AssignedTenant);
        Y_ABORT_UNLESS(!tenant->AssignedSlots.contains(slot));
        slot->AssignedTenant = tenant;
        slot->Label = label;
        tenant->AssignedSlots.insert(slot);

        LOG_NOTICE_S(ctx, NKikimrServices::TENANT_POOL,
                     LogPrefix << "attached tenant " << tenant->Name << " to slot "
                     << slot->Id << " with label " << label);
    }

    void DetachSlot(TDynamicSlotInfo::TPtr slot, const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(slot->AssignedTenant);
        Y_ABORT_UNLESS(slot->AssignedTenant->AssignedSlots.contains(slot));

        LOG_NOTICE_S(ctx, NKikimrServices::TENANT_POOL,
                     LogPrefix << "detach tenant " << slot->AssignedTenant->Name
                     << " from slot " << slot->Id << " with label " << slot->Label);

        slot->AssignedTenant->AssignedSlots.erase(slot);
        slot->AssignedTenant = nullptr;
        slot->Label = "";
    }

    void DetachAllSlots(TTenantInfo::TPtr tenant, const TActorContext &ctx)
    {
        while (tenant->AssignedSlots)
            DetachSlot(*tenant->AssignedSlots.begin(), ctx);
    }

    THolder<TEvTenantPool::TEvTenantPoolStatus> BuildStatusEvent(bool listStatic = false)
    {
        THolder<TEvTenantPool::TEvTenantPoolStatus> ev = MakeHolder<TEvTenantPool::TEvTenantPoolStatus>();
        if (listStatic) {
            for (auto& pr : Config->StaticSlots) {
                NKikimrTenantPool::TSlotConfig& slotConfig = pr.second;
                NKikimrTenantPool::TSlotStatus& slotStatus = *ev->Record.AddSlots();
                slotStatus.SetId(slotConfig.GetId());
                slotStatus.SetType(slotConfig.GetType());
                slotStatus.SetAssignedTenant(slotConfig.GetTenantName());
                slotStatus.MutableResourceLimit()->CopyFrom(slotConfig.GetResourceLimit());
                slotStatus.SetLabel(Config->StaticSlotLabel);

                auto it = Tenants.find(slotConfig.GetTenantName());
                if (it != Tenants.end()) {
                    for (auto &pr : it->second->Attributes) {
                        auto &attr = *slotStatus.AddTenantAttributes();
                        attr.SetKey(pr.first);
                        attr.SetValue(pr.second);
                    }
                    slotStatus.SetState(it->second->State);
                    *slotStatus.MutableDomainKey() = NKikimrSubDomains::TDomainKey(it->second->DomainKey);
                }
            }
        }
        for (auto &pr : DynamicSlots)
            FillSlotStatus(pr.second, *ev->Record.AddSlots());
        ev->Record.SetNodeType(Config->NodeType);
        return ev;
    }

    void ProcessActiveAction(TDynamicSlotInfo::TPtr slot, const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(slot->ActiveAction);
        Y_ABORT_UNLESS(slot->ActiveAction->GetTypeRewrite() == TEvTenantPool::EvConfigureSlot);
        auto &rec = slot->ActiveAction->Get<TEvTenantPool::TEvConfigureSlot>()->Record;
        bool ready = true;
        bool updated = false;

        if (slot->AssignedTenant && slot->AssignedTenant->Name != rec.GetAssignedTenant()) {
            auto tenant = slot->AssignedTenant;
            DetachSlot(slot, ctx);
            UpdateTenant(tenant, ctx);
            updated = true;
        }

        if (rec.GetAssignedTenant()
            && (!slot->AssignedTenant || slot->AssignedTenant->Name != rec.GetAssignedTenant())) {
            auto tenant = MaybeAddTenant(rec.GetAssignedTenant());
            AttachSlot(slot, tenant, rec.GetLabel(), ctx);
            UpdateTenant(tenant, ctx);
            ready = tenant->LastStatus == TEvLocal::TEvTenantStatus::STARTED;
            updated = true;
        }

        if (updated)
            SendStatusUpdates(ctx);

        if (ready) {
            SendSlotStatus(slot, NKikimrTenantPool::SUCCESS, "", ctx);
            ProcessPendingActions({slot}, ctx);
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << "Bootstrap");

        SubscribeForConfig(ctx);

        TenantSlotBroker.TabletId = MakeTenantSlotBrokerID();

        for (auto &pr : Config->StaticSlots) {
            TTenantInfo::TPtr tenant = new TTenantInfo(pr.second.GetTenantName());
            tenant->StaticResourceLimit = pr.second.GetResourceLimit();
            tenant->HasStaticSlot = true;
            Tenants.emplace(tenant->Name, tenant);
        }

        for (auto &pr : Tenants)
            UpdateTenant(pr.second, ctx);

        if (!DynamicSlots.empty())
            TryToRegister(ctx);

        SendStatusUpdates(ctx);

        Become(&TThis::StateWork);
    }

    void SubscribeForConfig(const TActorContext &ctx)
    {
        auto kind = (ui32)NKikimrConsole::TConfigItem::MonitoringConfigItem;
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
                 new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(kind));
    }

    void ApplyConfig(const NKikimrConfig::TMonitoringConfig &config,
                     const TActorContext &ctx)
    {
        auto &staticSlotLabel = config.GetDatabaseLabels().GetStaticSlotLabelValue();
        bool modified = false;

        if (Config->StaticSlotLabel != staticSlotLabel) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "static slot label modified from "
                        << Config->StaticSlotLabel << " to " << staticSlotLabel);
            if (Config->StaticSlots.size())
                modified = true;
            Config->StaticSlotLabel = staticSlotLabel;
        }

        if (modified)
            SendStatusUpdates(ctx);
    }

    void HandlePoison(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << "HandlePoison");
        Die(ctx);
    }

    void Handle(TEvents::TEvSubscribe::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        StatusSubscribers.insert(ev->Sender);
        Send(ev->Sender, BuildStatusEvent(true));
    }

    void Handle(TEvents::TEvUnsubscribe::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        StatusSubscribers.erase(ev->Sender);
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx)
    {
        auto &rec = ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << "Got new monitoring config: "
                    << rec.GetConfig().ShortDebugString());

        ApplyConfig(rec.GetConfig().GetMonitoringConfig(), ctx);

        auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

        LOG_TRACE_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << "Send TEvConfigNotificationResponse: "
                    << resp->Record.ShortDebugString());

        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    }

    void Handle(TEvLocal::TEvTenantStatus::TPtr &ev, const TActorContext &ctx)
    {
        auto tenant = Tenants[ev->Get()->TenantName];
        Y_ABORT_UNLESS(tenant, "status for unknown tenant");
        Y_ABORT_UNLESS(!tenant->HasStaticSlot || ev->Get()->Status == TEvLocal::TEvTenantStatus::STARTED,
                 "Cannot start static tenant %s: %s", ev->Get()->TenantName.data(), ev->Get()->Error.data());

        bool modified = false;
        bool processPendingActions = false;
        TTenantInfo::EState state = tenant->State;
        tenant->LastStatus = ev->Get()->Status;
        THashSet<TDynamicSlotInfo::TPtr, TPtrHash> toProcess = tenant->AssignedSlots;
        switch(ev->Get()->Status) {
        case TEvLocal::TEvTenantStatus::STARTED:
            state = TTenantInfo::EState::TENANT_OK;
            if (!tenant->HasStaticSlot && !tenant->AssignedSlots.size()) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "started tenant " << tenant->Name
                            << " which should be stopped");
                UpdateTenant(tenant, ctx);
            } else if (!tenant->CompareLimit(ev->Get()->ResourceLimit)) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "started tenant " << tenant->Name
                            << " has wrong resource limit");
                UpdateTenant(tenant, ctx);
            } else {
                LOG_NOTICE_S(ctx, NKikimrServices::TENANT_POOL,
                             LogPrefix << "started tenant " << tenant->Name);
                if (tenant->Attributes != ev->Get()->Attributes) {
                    tenant->Attributes = std::move(ev->Get()->Attributes);
                    modified = true;
                }
                if (tenant->DomainKey != ev->Get()->DomainKey) {
                    tenant->DomainKey = ev->Get()->DomainKey;
                    modified = true;
                }
                SendTenantStatus(tenant, NKikimrTenantPool::SUCCESS, ctx);
                processPendingActions = true;
            }
            break;
        case TEvLocal::TEvTenantStatus::STOPPED:
            state = TTenantInfo::EState::TENANT_OK;
            if (tenant->AssignedSlots.size()) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << "stopped tenant " << tenant->Name
                            << "  which should be started");
                UpdateTenant(tenant, ctx);
            } else {
                LOG_NOTICE_S(ctx, NKikimrServices::TENANT_POOL,
                             LogPrefix << "stopped tenant " << tenant->Name);
            }
            break;
        case TEvLocal::TEvTenantStatus::UNKNOWN_TENANT:
            modified = true;
            state = TTenantInfo::EState::TENANT_UNKNOWN;
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "couldn't start unknown tenant "
                        << tenant->Name);
            SendTenantStatus(tenant, NKikimrTenantPool::UNKNOWN_TENANT,
                             ev->Get()->Error, ctx);
            DetachAllSlots(tenant, ctx);
            processPendingActions = true;
            break;
        }

        if (modified || tenant->State != state) {
            tenant->State = state;
            SendStatusUpdates(ctx);
        }

        if (processPendingActions) {
            ProcessPendingActions(toProcess, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->ClientId != TenantSlotBroker.Pipe)
            return;
        if (msg->Status == NKikimrProto::OK)
            return;
        HandlePipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        if (msg->ClientId != TenantSlotBroker.Pipe)
            return;
        HandlePipeDestroyed(ctx);
    }

    void Handle(TEvTenantPool::TEvGetStatus::TPtr &ev, const TActorContext &ctx)
    {
        auto& rec = ev->Get()->Record;
        auto event = BuildStatusEvent(rec.GetListStaticSlots());
        ctx.Send(ev->Sender, std::move(event), 0, ev->Cookie);
    }

    void Handle(TEvTenantPool::TEvConfigureSlot::TPtr &ev, const TActorContext &ctx)
    {
        auto &rec = ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                    LogPrefix << ev->Sender << " configures slot '" << rec.GetSlotId()
                    << "' for tenant '" << rec.GetAssignedTenant() << "'");

        if (ev->Sender != TenantSlotBroker.ActorId) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "configure sender " << ev->Sender << " doesn't own pool");
            SendConfigureError(ev, rec.GetSlotId(), NKikimrTenantPool::NOT_OWNER,
                               "pool is not owned by request sender", ctx);
            return;
        }

        if (!DynamicSlots.contains(rec.GetSlotId())) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                        LogPrefix << "got configure for unknown slot " <<  rec.GetSlotId());
            SendConfigureError(ev, rec.GetSlotId(), NKikimrTenantPool::UNKNOWN_SLOT,
                               "unknown slot id", ctx);
            return;
        }

        auto slot = DynamicSlots[rec.GetSlotId()];
        if (slot->ActiveAction) {
            slot->PendingActions.emplace(ev.Release());
            return;
        }

        slot->ActiveAction = ev.Release();
        ProcessActiveAction(slot, ctx);
    }

    void Handle(TEvTenantPool::TEvTakeOwnership::TPtr &ev, const TActorContext &ctx)
    {
        const auto& record = ev->Get()->Record;
        const bool isOldStyle = !record.HasGeneration() && !record.HasSeqNo();
        const ui64 generation = isOldStyle ? 0 : record.GetGeneration();
        const ui64 seqNo = isOldStyle ? 0 : record.GetSeqNo();

        if (isOldStyle || generation > TenantSlotBroker.Generation || seqNo == TenantSlotBroker.SeqNo) {
            if (seqNo == Max<ui64>()) {
                NTabletPipe::CloseAndForgetClient(SelfId(), TenantSlotBroker.Pipe);
                TryToRegister(ctx);

                return;
            } else if (!isOldStyle && seqNo != TenantSlotBroker.SeqNo) {
                return;
            }

            TenantSlotBroker.Generation = generation;

            if (TenantSlotBroker.ActorId != ev->Sender) {
                if (TenantSlotBroker.ActorId) {
                    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                                LogPrefix << TenantSlotBroker.ActorId << " lost ownership");
                    ctx.Send(TenantSlotBroker.ActorId, new TEvTenantPool::TEvLostOwnership);
                }

                TenantSlotBroker.ActorId = ev->Sender;
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_POOL,
                            LogPrefix << TenantSlotBroker.ActorId << " took ownership");
            }

            auto event = BuildStatusEvent();
            ctx.Send(ev->Sender, std::move(event), 0, ev->Cookie);
        }
    }

    TString EventToString(IEventHandle *ev) {
        if (!ev)
            return "no event";
        return Sprintf("%" PRIx32 " event %s", ev->GetTypeRewrite(),
                       ev->ToString().data());
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx)
    {
        TStringStream str;
        str << NMonitoring::HTTPOKHTML;
        HTML(str) {
            PRE() {
                str << "Tenant pool for " << DomainName << Endl
                    << "LocalID: " << LocalID << Endl
                    << "TenantSlotBroker:" << Endl
                    << "   TabletId: " << TenantSlotBroker.TabletId << Endl
                    << "   Generation: " << TenantSlotBroker.Generation << Endl
                    << "   ActorId: " << TenantSlotBroker.ActorId << Endl << Endl;

                str << "Config:" << Endl
                    << "   IsEnabled: " << Config->IsEnabled << Endl
                    << "   NodeType: " << Config->NodeType << Endl
                    << "   StaticSlotLabel: " << Config->StaticSlotLabel << Endl
                    << "   StaticSlots:" << Endl;
                for (auto &pr : Config->StaticSlots)
                    str << "      " << pr.second.ShortDebugString() << Endl;

                str << Endl;

                str << "Tenants:" << Endl;
                for (auto &pr : Tenants) {
                    str << " - Name: " << pr.second->Name << Endl
                        << "   HasStaticSlot: " << pr.second->HasStaticSlot << Endl
                        << "   StaticResourceLimit: " << pr.second->StaticResourceLimit.ShortDebugString() << Endl
                        << "   ResourceLimit: " << pr.second->ResourceLimit.ShortDebugString() << Endl
                        << "   LastStatus: " << (int)pr.second->LastStatus << Endl
                        << "   State: " << (int)pr.second->State << Endl
                        << "   DomainKey: " << pr.second->DomainKey << Endl;

                    str << "   AssignedSlots:" << Endl;
                    for (auto &slot : pr.second->AssignedSlots)
                        str << "     " << slot->Id << Endl;
                }
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0,
                                                      NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::TENANT_POOL);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFuncTraced(TEvents::TEvSubscribe, Handle);
            HFuncTraced(TEvents::TEvUnsubscribe, Handle);
            HFuncTraced(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFuncTraced(TEvLocal::TEvTenantStatus, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFuncTraced(TEvTenantPool::TEvGetStatus, Handle);
            HFuncTraced(TEvTenantPool::TEvConfigureSlot, Handle);
            HFuncTraced(TEvTenantPool::TEvTakeOwnership, Handle);
            HFuncTraced(NMon::TEvHttpInfo, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

class TTenantPool : public TActorBootstrapped<TTenantPool> {
    TTenantPoolConfig::TPtr Config;
    TActorId LocalID;
    THashMap<TString, TActorId> DomainTenantPools;
    const TString DomainPrefix;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::TENANT_POOL_ACTOR;
    }

    TTenantPool(TTenantPoolConfig::TPtr config)
        : Config(config)
        , DomainPrefix("/domain/")
    {
    }

    void HandlePoison(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_POOL, "TTenantPool: HandlePoison");
        if (LocalID)
            ctx.Send(LocalID, new TEvents::TEvPoisonPill());
        for (auto &pr : DomainTenantPools)
            ctx.Send(pr.second, new TEvents::TEvPoisonPill());
        Die(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx)
    {
        NMon::TEvHttpInfo *msg = ev->Get();
        // Check for index page.
        if (msg->Request.GetPathInfo() == "" || msg->Request.GetPathInfo() == "/") {
            TStringStream str;
            if (!DomainTenantPools.size()) {
                str << "No domains are handled";
            } else if (DomainTenantPools.size() == 1) {
                ctx.Send(ev->Forward(DomainTenantPools.begin()->second));
                return;
            } else {
                for (auto &pr : DomainTenantPools) {
                    str << "<br/><a href=\"" << DomainPrefix << pr.first << "\">"
                        << pr.first << "</a>";
                }
            }
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0,
                                                          NMon::IEvHttpInfoRes::EContentType::Html));
        } else if (msg->Request.GetPathInfo().StartsWith(DomainPrefix)) {
            TString domain{msg->Request.GetPathInfo().substr(DomainPrefix.size())};
            if (!DomainTenantPools.contains(domain)) {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Unknown domain " + domain, 0,
                                                              NMon::IEvHttpInfoRes::EContentType::Html));
            } else {
                ctx.Send(ev->Forward(DomainTenantPools.find(domain)->second));
            }
        } else {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("Bad link", 0,
                                                          NMon::IEvHttpInfoRes::EContentType::Html));
        }
    }

    TTenantPoolConfig::TPtr CreateDomainConfig()
    {
        TTenantPoolConfig::TPtr res = new TTenantPoolConfig;
        res->NodeType = Config->NodeType;
        res->StaticSlotLabel = Config->StaticSlotLabel;
        return res;
    }

    void Handle(TEvents::TEvSubscribe::TPtr &ev, const TActorContext &ctx) {
        for (auto &pr : DomainTenantPools)
            ctx.Send(new IEventHandle(pr.second, ev->Sender, new TEvents::TEvSubscribe()));
    }

    void Handle(TEvents::TEvUnsubscribe::TPtr &ev, const TActorContext &ctx) {
        for (auto &pr : DomainTenantPools)
            ctx.Send(new IEventHandle(pr.second, ev->Sender, new TEvents::TEvUnsubscribe()));
    }

    void Handle(TEvLocal::TEvLocalDrainNode::TPtr &ev, const TActorContext &ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TENANT_POOL, "Forward drain node to local.");
        ctx.Send(ev->Forward(LocalID));
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_POOL, "TTenantPool::Bootstrap");

        NActors::TMon *mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "tenant_pool", "Tenant Pool", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }

        if (!Config->IsEnabled) {
            LOG_DEBUG(ctx, NKikimrServices::TENANT_POOL, "TenantPool is disabled");
            return;
        }

        auto *local = CreateLocal(Config->LocalConfig.Get());
        LocalID = ctx.ExecutorThread.RegisterActor(local, TMailboxType::ReadAsFilled, 0);
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(MakeLocalID(SelfId().NodeId()), LocalID);

        THashMap<TString, TTenantPoolConfig::TPtr> domainConfigs;

        for (auto &pr : Config->StaticSlots) {
            TString domain = TString(ExtractDomain(pr.second.GetTenantName()));
            Y_ABORT_UNLESS(domain, "cannot extract domain from tenant name");
            if (!domainConfigs.contains(domain))
                domainConfigs[domain] = CreateDomainConfig();
            domainConfigs[domain]->AddStaticSlot(pr.second);
        }

        auto domains = AppData(ctx)->DomainsInfo;
        auto *domain = domains->GetDomain();
        for (auto &pr : domainConfigs) {
            Y_ABORT_UNLESS(domain->Name == pr.first, "unknown domain %s in Tenant Pool config", pr.first.data());
            auto aid = ctx.RegisterWithSameMailbox(new TDomainTenantPool(pr.first, LocalID, pr.second));
            DomainTenantPools[pr.first] = aid;
            auto serviceId = MakeTenantPoolID(SelfId().NodeId());
            ctx.ExecutorThread.ActorSystem->RegisterLocalService(serviceId, aid);
        }

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            HFunc(TEvents::TEvSubscribe, Handle);
            HFunc(TEvents::TEvUnsubscribe, Handle);
            HFunc(NMon::TEvHttpInfo, Handle);
            HFunc(TEvLocal::TEvLocalDrainNode, Handle);
        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

} // anonymous namespace

TTenantPoolConfig::TTenantPoolConfig(TLocalConfig::TPtr localConfig)
    : TTenantPoolConfig(NKikimrTenantPool::TTenantPoolConfig(),
                        NKikimrConfig::TMonitoringConfig(),
                        localConfig)
{
}

TTenantPoolConfig::TTenantPoolConfig(const NKikimrTenantPool::TTenantPoolConfig &config,
                                     const NKikimrConfig::TMonitoringConfig &monCfg,
                                     TLocalConfig::TPtr localConfig)
    : LocalConfig(localConfig)
{
    for (auto &slot : config.GetSlots()) {
        if (!slot.GetIsDynamic())
            AddStaticSlot(slot);
    }
    IsEnabled = config.GetIsEnabled();
    NodeType = config.GetNodeType();

    StaticSlotLabel = monCfg.GetDatabaseLabels().GetStaticSlotLabelValue();
}

TTenantPoolConfig::TTenantPoolConfig(const NKikimrTenantPool::TTenantPoolConfig &config,
                                     TLocalConfig::TPtr localConfig)
    : TTenantPoolConfig(config,
                        NKikimrConfig::TMonitoringConfig(),
                        localConfig)
{
}

void TTenantPoolConfig::AddStaticSlot(const NKikimrTenantPool::TSlotConfig &slot)
{
    TString name = CanonizePath(slot.GetTenantName());
    Y_ABORT_UNLESS(IsEnabled);
    Y_ABORT_UNLESS(!StaticSlots.contains(name),
             "two static slots for the same tenant '%s'", name.data());
    StaticSlots[name] = slot;
    StaticSlots[name].SetTenantName(name);
}

void TTenantPoolConfig::AddStaticSlot(const TString &tenant,
                                      const NKikimrTabletBase::TMetrics &limit)
{
    NKikimrTenantPool::TSlotConfig slot;
    slot.SetTenantName(tenant);
    slot.SetIsDynamic(false);
    slot.MutableResourceLimit()->CopyFrom(limit);
    AddStaticSlot(slot);
}

IActor* CreateTenantPool(TTenantPoolConfig::TPtr config)
{
    return new TTenantPool(config);
}

} // namespace NKikimr
