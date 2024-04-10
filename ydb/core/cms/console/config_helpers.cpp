#include "config_helpers.h"
#include "configs_dispatcher.h"
#include "console.h"
#include "util.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mind/tenant_pool.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/join.h>
#include <util/system/hostname.h>

namespace NKikimr::NConsole {

namespace {

enum class EAction {
    GET_NODE_CONFIG,
    REPLACE_SUBSCRIPTION,
    ADD_SUBSCRIPTION,
    REMOVE_SUBSCRIPTION
};

class TConfigHelper : public TActorBootstrapped<TConfigHelper> {
private:
    using TBase = TActorBootstrapped<TConfigHelper>;

    struct TEvPrivate {
        enum EEv {
            EvRetryPoolStatus = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryPoolStatus : public TEventLocal<TEvRetryPoolStatus, EvRetryPoolStatus> {
            // empty
        };
    };

    ui64 TabletId;
    TActorId ServiceId;
    TString Tenant;
    bool DetectTenant;
    TString NodeType;
    TVector<ui32> ConfigItemKinds;
    ui64 SubscriptionId;
    EAction Action;
    TActorId OwnerId;
    TActorId Pipe;
    ui64 Cookie;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_CONFIGS_SUBSCRIBER;
    }

    // Add/replace subscription.
    TConfigHelper(ui64 tabletId,
                  TActorId serviceId,
                  const TVector<ui32> &configItemKinds,
                  const TString &tenant,
                  TActorId ownerId,
                  bool replace,
                  ui64 cookie)
        : TabletId(tabletId)
        , ServiceId(serviceId)
        , Tenant(tenant)
        , DetectTenant(false)
        , ConfigItemKinds(configItemKinds)
        , SubscriptionId(0)
        , Action(replace ? EAction::REPLACE_SUBSCRIPTION : EAction::ADD_SUBSCRIPTION)
        , OwnerId(ownerId)
        , Cookie(cookie)
    {
    }

    // Add/replace subscription.
    TConfigHelper(ui64 tabletId,
                  TActorId serviceId,
                  const TVector<ui32> &configItemKinds,
                  TActorId ownerId,
                  bool replace,
                  ui64 cookie)
        : TabletId(tabletId)
        , ServiceId(serviceId)
        , DetectTenant(true)
        , ConfigItemKinds(configItemKinds)
        , SubscriptionId(0)
        , Action(replace ? EAction::REPLACE_SUBSCRIPTION : EAction::ADD_SUBSCRIPTION)
        , OwnerId(ownerId)
        , Cookie(cookie)
    {
    }

    // Remove subscription.
    TConfigHelper(ui64 subscriptionId,
                  TActorId ownerId,
                  ui64 cookie)
        : TabletId(0)
        , DetectTenant(false)
        , SubscriptionId(subscriptionId)
        , Action(EAction::REMOVE_SUBSCRIPTION)
        , OwnerId(ownerId)
        , Cookie(cookie)
    {
    }

    // Get node config.
    TConfigHelper(const TVector<ui32> &configItemKinds,
                  TActorId ownerId,
                  ui64 cookie)
        : TabletId(0)
        , DetectTenant(true)
        , ConfigItemKinds(configItemKinds)
        , SubscriptionId(0)
        , Action(EAction::GET_NODE_CONFIG)
        , OwnerId(ownerId)
        , Cookie(cookie)
    {
    }

    // Get node config.
    TConfigHelper(const TVector<ui32> &configItemKinds,
                  const TString &tenant,
                  TActorId ownerId,
                  ui64 cookie)
        : TabletId(0)
        , Tenant(tenant)
        , DetectTenant(false)
        , ConfigItemKinds(configItemKinds)
        , SubscriptionId(0)
        , Action(EAction::GET_NODE_CONFIG)
        , OwnerId(ownerId)
        , Cookie(cookie)
    {
    }

    void Die(const TActorContext &ctx) override
    {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigHelper Die");

        if (Pipe)
            NTabletPipe::CloseClient(ctx, Pipe);
        TBase::Die(ctx);
    }

    void OnPipeDestroyed(const TActorContext &ctx)
    {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }

        SendSubscriptionRequest(ctx);
    }

    void OpenPipe(const TActorContext &ctx)
    {
        auto console = MakeConsoleID();

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, console, pipeConfig);
        Pipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

    void SendPoolStatusRequest(const TActorContext &ctx)
    {
        auto tenantPool = MakeTenantPoolID(ctx.SelfID.NodeId());
        ctx.Send(tenantPool, new TEvTenantPool::TEvGetStatus(true), IEventHandle::FlagTrackDelivery);
    }

    void BuildSubscription(NKikimrConsole::TSubscription &subscription)
    {
        if (TabletId)
            subscription.MutableSubscriber()->SetTabletId(TabletId);
        else
            ActorIdToProto(ServiceId,
                               subscription.MutableSubscriber()->MutableServiceId());
        subscription.MutableOptions()->SetNodeId(SelfId().NodeId());
        subscription.MutableOptions()->SetHost(FQDNHostName());
        subscription.MutableOptions()->SetTenant(Tenant);
        subscription.MutableOptions()->SetNodeType(NodeType);
        for (auto &kind : ConfigItemKinds)
            subscription.AddConfigItemKinds(kind);
    }

    void SendSubscriptionRequest(const TActorContext &ctx)
    {
        if (!Pipe)
            OpenPipe(ctx);

        if (Action == EAction::REPLACE_SUBSCRIPTION) {
            auto request = MakeHolder<TEvConsole::TEvReplaceConfigSubscriptionsRequest>();
            BuildSubscription(*request->Record.MutableSubscription());

            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigHelper send TEvReplaceConfigSubscriptionsRequest: "
                        << request->Record.ShortDebugString());

            NTabletPipe::SendData(ctx, Pipe, request.Release(), Cookie);
        } else if (Action == EAction::ADD_SUBSCRIPTION) {
            auto request = MakeHolder<TEvConsole::TEvAddConfigSubscriptionRequest>();
            BuildSubscription(*request->Record.MutableSubscription());

            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigHelper send TEvAddConfigSubscriptionRequest: "
                        << request->Record.ShortDebugString());

            NTabletPipe::SendData(ctx, Pipe, request.Release(), Cookie);
        } else if (Action == EAction::REMOVE_SUBSCRIPTION) {
            auto request = MakeHolder<TEvConsole::TEvRemoveConfigSubscriptionRequest>();
            request->Record.SetSubscriptionId(SubscriptionId);

            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigHelper send TEvRemoveConfigSubscriptionRequest: "
                        << request->Record.ShortDebugString());

            NTabletPipe::SendData(ctx, Pipe, request.Release(), Cookie);
        } else if (Action == EAction::GET_NODE_CONFIG) {
            auto request = MakeHolder<TEvConsole::TEvGetNodeConfigRequest>();
            request->Record.MutableNode()->SetNodeId(SelfId().NodeId());
            request->Record.MutableNode()->SetHost(FQDNHostName());
            request->Record.MutableNode()->SetTenant(Tenant);
            request->Record.MutableNode()->SetNodeType(NodeType);
            for (auto &kind : ConfigItemKinds)
                request->Record.AddItemKinds(kind);

            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigHelper send TEvGetNodeConfigRequest: "
                        << request->Record.ShortDebugString());

            NTabletPipe::SendData(ctx, Pipe, request.Release(), Cookie);
        } else {
            Y_ABORT("unknown action");
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigHelper Bootstrap"
                    << " tabletid=" << TabletId
                    << " serviceid=" << ServiceId
                    << " subscriptionid=" << SubscriptionId
                    << " action=" << (ui32)Action
                    << " tenant=" << Tenant
                    << " detecttenant=" << DetectTenant
                    << " kinds=" << JoinSeq(",", ConfigItemKinds));
        Become(&TThis::StateWork);
        if (Action == EAction::REPLACE_SUBSCRIPTION
            || Action == EAction::ADD_SUBSCRIPTION
            || Action == EAction::GET_NODE_CONFIG)
            SendPoolStatusRequest(ctx);
        else if (Action == EAction::REMOVE_SUBSCRIPTION)
            SendSubscriptionRequest(ctx);
        else
            Y_ABORT("unknown action");
    }

    void Handle(TEvConsole::TEvAddConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx) {
        ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvConsole::TEvGetNodeConfigResponse::TPtr &ev, const TActorContext &ctx) {
        ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvConsole::TEvRemoveConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx) {
        if (OwnerId)
            ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev, const TActorContext &ctx) {
        ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigHelper connection "
                    << ((ev->Get()->Status == NKikimrProto::OK) ? "established" : "failed"));

        if (ev->Get()->Status != NKikimrProto::OK) {
            OnPipeDestroyed(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &/*ev*/, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigHelper TEvTabletPipe::TEvClientDestroyed");
        OnPipeDestroyed(ctx);
    }

    void Handle(TEvPrivate::TEvRetryPoolStatus::TPtr &, const TActorContext &ctx) {
        SendPoolStatusRequest(ctx);
    }

    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigHelper got status from TenantPool: "
                    << rec.ShortDebugString());

        NodeType = rec.GetNodeType();
        if (DetectTenant) {
            THashSet<TString> tenants;
            for (auto &slot : rec.GetSlots())
                tenants.insert(slot.GetAssignedTenant());
            if (tenants.empty())
                Tenant = "<none>";
            else if (tenants.size() == 1) {
                Tenant = CanonizePath(*tenants.begin());
            } else
                Tenant = "<multiple>";
        }

        SendSubscriptionRequest(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->SourceType) {
            case TEvTenantPool::TEvGetStatus::EventType: {
                LOG_WARN_S(ctx, NKikimrServices::CMS_CONFIGS,
                           "TConfigHelper cannot deliver message to domain tenant, will retry");
                ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRetryPoolStatus);
                break;
            }
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvAddConfigSubscriptionResponse, Handle);
            HFunc(TEvConsole::TEvGetNodeConfigResponse, Handle);
            HFunc(TEvConsole::TEvRemoveConfigSubscriptionResponse, Handle);
            HFunc(TEvConsole::TEvReplaceConfigSubscriptionsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvPrivate::TEvRetryPoolStatus, Handle);
            HFunc(TEvTenantPool::TEvTenantPoolStatus, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

} // anonymous namespace

IActor *CreateNodeConfigCourier(TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(TVector<ui32>(), owner, cookie);
}

IActor *CreateNodeConfigCourier(ui32 configItemKind,
                                TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(TVector<ui32>({configItemKind}), owner, cookie);
}

IActor *CreateNodeConfigCourier(const TVector<ui32> &configItemKinds,
                                TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(configItemKinds, owner, cookie);
}

IActor *CreateNodeConfigCourier(const TString &tenant,
                                TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(TVector<ui32>({}), tenant, owner, cookie);
}

IActor *CreateNodeConfigCourier(ui32 configItemKind,
                                const TString &tenant,
                                TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(TVector<ui32>({configItemKind}), tenant, owner, cookie);
}

IActor *CreateNodeConfigCourier(const TVector<ui32> &configItemKinds,
                                const TString &tenant,
                                TActorId owner,
                                ui64 cookie)
{
    return new TConfigHelper(configItemKinds, tenant, owner, cookie);
}

IActor *CreateConfigSubscriber(ui64 tabletId,
                               const TVector<ui32> &configItemKinds,
                               TActorId owner,
                               bool replace,
                               ui64 cookie)
{
    return new TConfigHelper(tabletId, TActorId(), configItemKinds, owner, replace, cookie);
}

IActor *CreateConfigSubscriber(ui64 tabletId,
                               const TVector<ui32> &configItemKinds,
                               const TString &tenant,
                               TActorId owner,
                               bool replace,
                               ui64 cookie)
{
    return new TConfigHelper(tabletId, TActorId(), configItemKinds, tenant, owner, replace, cookie);
}

IActor *CreateConfigSubscriber(TActorId serviceId,
                               const TVector<ui32> &configItemKinds,
                               TActorId owner,
                               bool replace,
                               ui64 cookie)
{
    return new TConfigHelper(0, serviceId, configItemKinds, owner, replace, cookie);
}

IActor *CreateConfigSubscriber(TActorId serviceId,
                               const TVector<ui32> &configItemKinds,
                               const TString &tenant,
                               TActorId owner,
                               bool replace,
                               ui64 cookie)
{
    return new TConfigHelper(0, serviceId, configItemKinds, tenant, owner, replace, cookie);
}

IActor *CreateSubscriptionEraser(ui64 subscriptionId,
                                 TActorId owner,
                                 ui64 cookie)
{
    return new TConfigHelper(subscriptionId, owner, cookie);
}

void SubscribeViaConfigDispatcher(const TActorContext &ctx,
                                  const TVector<ui32> &configItemKinds,
                                  TActorId owner,
                                  ui64 cookie)
{
    ctx.Send(
        MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(configItemKinds, owner),
        cookie);
}

void UnsubscribeViaConfigDispatcher(const TActorContext &ctx,
                                    TActorId owner,
                                    ui64 cookie)
{
    ctx.Send(
        MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest(owner),
        cookie);
}

} // namespace NKikimr::NConsole
