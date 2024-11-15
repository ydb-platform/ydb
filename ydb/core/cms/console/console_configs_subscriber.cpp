#include "console_configs_subscriber.h"
#include "console.h"
#include "util.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/mind/tenant_pool.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
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
    TConfigsSubscriber(
            const TActorId &ownerId,
            ui64 cookie,
            const TVector<ui32> &kinds,
            const NKikimrConfig::TAppConfig &currentConfig,
            bool processYaml,
            ui64 version,
            const TString &yamlConfig,
            const TMap<ui64, TString> &volatileYamlConfigs,
            const std::optional<TNodeInfo> explicitNodeInfo)
        : OwnerId(ownerId)
        , Cookie(cookie)
        , Kinds(kinds)
        , Generation(0)
        , NextGeneration(1)
        , LastOrder(0)
        , CurrentConfig(currentConfig)
        , ServeYaml(processYaml)
        , Version(version)
        , YamlConfig(yamlConfig)
        , VolatileYamlConfigs(volatileYamlConfigs)
    {
        if (ServeYaml && !YamlConfig.empty()) {
            YamlConfigVersion = NYamlConfig::GetVersion(YamlConfig);
            for (auto &[id, config] : VolatileYamlConfigs) {
                VolatileYamlConfigHashes[id] = THash<TString>()(config);
            }
        }

        if (explicitNodeInfo) {
            if (explicitNodeInfo->Tenant) {
                Tenant = explicitNodeInfo->Tenant;
            } else {
                Tenant = "<none>";
            }
            NodeType = explicitNodeInfo->NodeType;
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_CONFIGS_SUBSCRIBER;
    }

    void Bootstrap(const TActorContext &ctx) {
        if (!AppData()->DomainsInfo->Domain) {
            Send(OwnerId, new NConsole::TEvConsole::TEvConfigSubscriptionError(Ydb::StatusIds::GENERIC_ERROR, "Ambiguous domain (use --domain option)"), 0, Cookie);

            Die(ctx);

            return;
        }

        if (!Tenant) {
            SendPoolStatusRequest(ctx);
        } else {
            Subscribe(ctx);
        }
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::CMS_CONFIGS);

        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPrivate::TEvRetryPoolStatus, Handle);
            HFuncTraced(TEvTenantPool::TEvTenantPoolStatus, Handle);
            HFuncTraced(TEvConsole::TEvGetNodeConfigResponse, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionResponse, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionError, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionCanceled, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);

            default:
                Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void SendPoolStatusRequest(const TActorContext &ctx) {
        ctx.Send(MakeTenantPoolID(ctx.SelfID.NodeId()), new TEvTenantPool::TEvGetStatus(true), IEventHandle::FlagTrackDelivery);
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
            Send(OwnerId, new TEvConsole::TEvConfigSubscriptionError(rec.GetStatus().GetCode(), rec.GetStatus().GetReason()), 0, Cookie);

            Generation = 0;
            Die(ctx);
        }

        if (!FirstUpdateSent) {
            auto request = MakeHolder<TEvConsole::TEvGetNodeConfigRequest>();
            request->Record.MutableNode()->SetNodeId(SelfId().NodeId());
            request->Record.MutableNode()->SetHost(FQDNHostName());
            request->Record.MutableNode()->SetTenant(Tenant);
            request->Record.MutableNode()->SetNodeType(NodeType);
            for (auto &kind : Kinds) {
                request->Record.AddItemKinds(kind);
            }

            NTabletPipe::SendData(ctx, Pipe, request.Release(), Cookie);
        }
    }

    void Handle(TEvConsole::TEvGetNodeConfigResponse::TPtr &ev, const TActorContext &ctx) {
        if (!FirstUpdateSent) {
            ctx.ExecutorThread.Send(
                new NActors::IEventHandle(
                    SelfId(),
                    ev->Sender,
                    new NConsole::TEvConsole::TEvConfigSubscriptionNotification(
                        Generation,
                        ev->Get()->Record.GetConfig(),
                        THashSet<ui32>(Kinds.begin(), Kinds.end()))));
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

        Y_ABORT_UNLESS(Pipe);

        if (rec.GetOrder() != (LastOrder + 1)) {
            BLOG_I("Order mismatch, will resubscribe");

            Subscribe(ctx);

            return;
        }

        bool notChanged = true;

        if (ServeYaml) {
            if (!(rec.HasYamlConfigNotChanged() && rec.GetYamlConfigNotChanged())) {
                if (rec.HasYamlConfig()) {
                    YamlConfig = rec.GetYamlConfig();
                    YamlConfigVersion = NYamlConfig::GetVersion(YamlConfig);
                }

                notChanged = false;
            }

            if (rec.VolatileConfigsSize() != VolatileYamlConfigs.size()) {
                notChanged = false;
            }

            TMap<ui64, TString> newVolatileYamlConfigs;
            TMap<ui64, ui64> newVolatileYamlConfigHashes;
            for (auto &volatileConfig : rec.GetVolatileConfigs()) {
                if (volatileConfig.HasNotChanged() && volatileConfig.GetNotChanged()) {
                    Y_ASSERT(VolatileYamlConfigs.contains(volatileConfig.GetId()));
                    newVolatileYamlConfigs[volatileConfig.GetId()] = std::move(VolatileYamlConfigs[volatileConfig.GetId()]);
                    newVolatileYamlConfigHashes[volatileConfig.GetId()] = VolatileYamlConfigHashes[volatileConfig.GetId()];
                } else {
                    notChanged = false;
                    newVolatileYamlConfigs[volatileConfig.GetId()] = volatileConfig.GetConfig();
                    newVolatileYamlConfigHashes[volatileConfig.GetId()] = THash<TString>()(volatileConfig.GetConfig());
                }
            }
            VolatileYamlConfigs = std::move(newVolatileYamlConfigs);
            VolatileYamlConfigHashes = std::move(newVolatileYamlConfigHashes);
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
            if (field && reflection->HasField(CurrentConfig, field)) {
                reflection->ClearField(&CurrentConfig, field);
            }
            if (field && reflection->HasField(CurrentDynConfig, field)) {
                reflection->ClearField(&CurrentDynConfig, field);
            }
        }

        CurrentConfig.MergeFrom(rec.GetConfig());
        CurrentDynConfig.MergeFrom(rec.GetConfig());
        if (newVersion.GetItems().empty())
            CurrentConfig.ClearVersion();
        else
            CurrentConfig.MutableVersion()->Swap(&newVersion);

        notChanged &= changes.empty();

        if (!notChanged || !FirstUpdateSent) {
            Send(OwnerId, new TEvConsole::TEvConfigSubscriptionNotification(
                     Generation,
                     CurrentConfig,
                     changes,
                     YamlConfig,
                     VolatileYamlConfigs,
                     CurrentDynConfig),
                IEventHandle::FlagTrackDelivery, Cookie);

            FirstUpdateSent = true;
        }

        LastOrder++;
    }

    void Handle(NConsole::TEvConsole::TEvConfigSubscriptionCanceled::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        if (rec.GetGeneration() != Generation) {
            BLOG_I("Generation mismatch for TEvConfigSubscriptionCanceled");

            return;
        }

        Y_ABORT_UNLESS(Pipe);

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
        request->Record.SetServeYaml(ServeYaml);
        request->Record.SetYamlApiVersion(Version);

        for (auto &kind : Kinds)
            request->Record.AddConfigItemKinds(kind);

        if (CurrentConfig.HasVersion())
            request->Record.MutableKnownVersion()->CopyFrom(CurrentConfig.GetVersion());

        if (ServeYaml) {
            if (!YamlConfig.empty()) {
                request->Record.SetYamlVersion(YamlConfigVersion);
                for (auto &[id, hash] : VolatileYamlConfigHashes) {
                    auto *item = request->Record.AddVolatileYamlVersion();
                    item->SetId(id);
                    item->SetHash(hash);
                }
            }
        }

        NTabletPipe::SendData(ctx, Pipe, request.Release());
    }

    void OnPipeDestroyed(const TActorContext &ctx) {
        Pipe = TActorId();

        Subscribe(ctx);
    }

    void OpenPipe(const TActorContext &ctx) {
        auto console = MakeConsoleID();

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, console, pipeConfig);
        Pipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

private:
    const TActorId OwnerId;
    ui64 Cookie;
    const TVector<ui32> Kinds;
    bool FirstUpdateSent = false;

    ui64 Generation;
    ui64 NextGeneration;
    ui64 LastOrder;

    NKikimrConfig::TAppConfig CurrentConfig;
    NKikimrConfig::TAppConfig CurrentDynConfig;

    bool ServeYaml = false;
    ui64 Version;
    TString YamlConfig;
    TMap<ui64, TString> VolatileYamlConfigs;
    ui64 YamlConfigVersion = 0;
    TMap<ui64, ui64> VolatileYamlConfigHashes;

    TString Tenant;
    TString NodeType;

    TActorId Pipe;
};

IActor *CreateConfigsSubscriber(
    const TActorId &ownerId,
    const TVector<ui32> &kinds,
    const NKikimrConfig::TAppConfig &currentConfig,
    ui64 cookie,
    bool processYaml,
    ui64 version,
    const TString &yamlConfig,
    const TMap<ui64, TString> &volatileYamlConfigs,
    const std::optional<TNodeInfo> explicitNodeInfo)
{
    return new TConfigsSubscriber(
        ownerId,
        cookie,
        kinds,
        currentConfig,
        processYaml,
        version,
        yamlConfig,
        volatileYamlConfigs,
        explicitNodeInfo);
}

} // namespace NKikimr::NConsole
