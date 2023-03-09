#include "console_configs_subscriber.h"
#include "console.h"
#include "config_index.h"
#include "util.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mind/tenant_pool.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <util/system/hostname.h>
#include <util/generic/ptr.h>

#include <utility>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)

namespace NKikimr::NConsole {

class TConfigsSubscriber : public TActorBootstrapped<TConfigsSubscriber> {
private:
    using TBase = TActorBootstrapped<TConfigsSubscriber>;

    struct TEvPrivate {
        enum EEv {
            EvRetryPoolStatus = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvRetryPoolStatus : public TEventLocal<TEvRetryPoolStatus, EvRetryPoolStatus> {
            // empty
        };
    };

public:
    TConfigsSubscriber(const TActorId &ownerId, const TVector<ui32> &kinds, const NKikimrConfig::TAppConfig &currentConfig)
        : OwnerId(ownerId)
        , Kinds(kinds)
        , Generation(0)
        , NextGeneration(1)
        , LastOrder(0)
        , CurrentConfig(currentConfig) {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_CONFIGS_SUBSCRIBER;
    }

    void Bootstrap(const TActorContext &ctx) {
        auto dinfo = AppData(ctx)->DomainsInfo;
        if (dinfo->Domains.size() != 1) {
            Send(OwnerId, new NConsole::TEvConsole::TEvConfigSubscriptionError(Ydb::StatusIds::GENERIC_ERROR, "Ambiguous domain (use --domain option)"));

            Die(ctx);

            return;
        }

        DomainUid = dinfo->Domains.begin()->second->DomainUid;
        StateStorageGroup = dinfo->GetDefaultStateStorageGroup(DomainUid);

        SendPoolStatusRequest(ctx);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::CMS_CONFIGS);

        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPrivate::TEvRetryPoolStatus, Handle);
            HFuncTraced(TEvTenantPool::TEvTenantPoolStatus, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionResponse, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionError, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionCanceled, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);

            default:
                Y_FAIL("unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void SendPoolStatusRequest(const TActorContext &ctx) {
        ctx.Send(MakeTenantPoolID(ctx.SelfID.NodeId(), DomainUid), new TEvTenantPool::TEvGetStatus(true),
            IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvPrivate::TEvRetryPoolStatus::TPtr &/*ev*/, const TActorContext &ctx) {
        SendPoolStatusRequest(ctx);
    }

    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        NodeType = rec.GetNodeType();

        THashSet<TString> tenants;
        for (auto &slot : rec.GetSlots())
            tenants.insert(slot.GetAssignedTenant());

        if (tenants.size() == 1)
            Tenant = CanonizePath(*tenants.begin());
        else
            Tenant = tenants.empty() ? "<none>" : "<multiple>";

        Subscribe(ctx);
    }

    void Handle(TEvConsole::TEvConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        if (rec.GetGeneration() != Generation) {
            BLOG_I("Generation mismatch for TEvConfigSubscriptionResponse");

            return;
        }

        if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Send(OwnerId, new TEvConsole::TEvConfigSubscriptionError(rec.GetStatus().GetCode(), rec.GetStatus().GetReason()));

            Generation = 0;
            Die(ctx);
        }
    }

    void Handle(TEvConsole::TEvConfigSubscriptionError::TPtr &ev, const TActorContext &ctx) {
        NActors::TActivationContext::Send(ev->Forward(OwnerId));

        Generation = 0;
        Die(ctx);
    }

    void Handle(NConsole::TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        if (rec.GetGeneration() != Generation) {
            BLOG_I("Generation mismatch for TEvConfigSubscriptionNotification");

            return;
        }

        Y_VERIFY(Pipe);

        if (rec.GetOrder() != (LastOrder + 1)) {
            BLOG_I("Order mismatch, will resubscribe");

            Subscribe(ctx);

            return;
        }

        NKikimrConfig::TConfigVersion newVersion(rec.GetConfig().GetVersion());
        THashSet<ui32> changes(rec.GetAffectedKinds().begin(), rec.GetAffectedKinds().end());
        for (auto &item : CurrentConfig.GetVersion().GetItems()) {
            if (!changes.contains(item.GetKind()))
                newVersion.AddItems()->CopyFrom(item);
        }

        auto *desc1 = CurrentConfig.GetDescriptor();
        auto *reflection = CurrentConfig.GetReflection();
        for (auto kind : rec.GetAffectedKinds()) {
            auto *field = desc1->FindFieldByNumber(kind);
            if (field && reflection->HasField(CurrentConfig, field))
                reflection->ClearField(&CurrentConfig, field);
        }

        CurrentConfig.MergeFrom(rec.GetConfig());
        if (newVersion.GetItems().empty())
            CurrentConfig.ClearVersion();
        else
            CurrentConfig.MutableVersion()->Swap(&newVersion);

        Send(OwnerId, new TEvConsole::TEvConfigSubscriptionNotification(Generation, CurrentConfig, changes),
             IEventHandle::FlagTrackDelivery);

        LastOrder++;
    }

    void Handle(NConsole::TEvConsole::TEvConfigSubscriptionCanceled::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        if (rec.GetGeneration() != Generation) {
            BLOG_I("Generation mismatch for TEvConfigSubscriptionCanceled");

            return;
        }

        Y_VERIFY(Pipe);

        Subscribe(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Status != NKikimrProto::OK)
            OnPipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &/*ev*/, const TActorContext &ctx) {
        OnPipeDestroyed(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->SourceType) {
            case TEvTenantPool::TEvGetStatus::EventType: {
                ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRetryPoolStatus);
                break;
            }

            default:
                Die(ctx);
        }
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &/*ev*/, const TActorContext &ctx) {
        Die(ctx);
    }

protected:
    void Die(const TActorContext &ctx) override {
        if (Pipe) {
            if (Generation != 0)
                NTabletPipe::SendData(ctx, Pipe, new NConsole::TEvConsole::TEvConfigSubscriptionCanceled(Generation));

            NTabletPipe::CloseClient(ctx, Pipe);
        }

        TBase::Die(ctx);
    }

private:
    void Subscribe(const TActorContext &ctx) {
        Generation = 0;
        LastOrder = 0;

        if (!Pipe)
            OpenPipe(ctx);

        auto request = MakeHolder<TEvConsole::TEvConfigSubscriptionRequest>();

        request->Record.SetGeneration(Generation = NextGeneration++);
        request->Record.MutableOptions()->SetNodeId(SelfId().NodeId());
        request->Record.MutableOptions()->SetTenant(Tenant);
        request->Record.MutableOptions()->SetNodeType(NodeType);
        request->Record.MutableOptions()->SetHost(FQDNHostName());

        for (auto &kind : Kinds)
            request->Record.AddConfigItemKinds(kind);

        if (CurrentConfig.HasVersion())
            request->Record.MutableKnownVersion()->CopyFrom(CurrentConfig.GetVersion());

        NTabletPipe::SendData(ctx, Pipe, request.Release());
    }

    void OnPipeDestroyed(const TActorContext &ctx) {
        Pipe = TActorId();

        Subscribe(ctx);
    }

    void OpenPipe(const TActorContext &ctx) {
        auto console = MakeConsoleID(StateStorageGroup);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, console, pipeConfig);
        Pipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

private:
    const TActorId OwnerId;
    const TVector<ui32> Kinds;

    ui64 Generation;
    ui64 NextGeneration;
    ui64 LastOrder;

    NKikimrConfig::TAppConfig CurrentConfig;

    TString Tenant;
    TString NodeType;

    ui32 DomainUid;
    ui32 StateStorageGroup;

    TActorId Pipe;
};

IActor *CreateConfigsSubscriber(const TActorId &ownerId, const TVector<ui32> &kinds, const NKikimrConfig::TAppConfig &currentConfig) {
    return new TConfigsSubscriber(ownerId, kinds, currentConfig);
}
}
