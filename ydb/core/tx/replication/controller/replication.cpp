#include "private_events.h"
#include "replication.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "tenant_resolver.h"
#include "util.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/actors/core/events.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimr::NReplication::NController {

class TReplication::TImpl {
    friend class TReplication;

    template <typename... Args>
    ITarget* CreateTarget(ui64 id, ETargetKind kind, Args&&... args) const {
        switch (kind) {
        case ETargetKind::Table:
            return new TTableTarget(ReplicationId, id, std::forward<Args>(args)...);
        }
    }

    void DiscoverTargets(const TActorContext& ctx) {
        switch (Config.GetTargetCase()) {
            case NKikimrReplication::TReplicationConfig::kEverything:
                return ErrorState("Not implemented");

            case NKikimrReplication::TReplicationConfig::kSpecific: {
                TVector<std::pair<TString, TString>> paths;
                for (const auto& target : Config.GetSpecific().GetTargets()) {
                    paths.emplace_back(target.GetSrcPath(), target.GetDstPath());
                }

                TargetDiscoverer = ctx.Register(CreateTargetDiscoverer(ctx.SelfID, ReplicationId, YdbProxy, std::move(paths)));
                break;
            }

            default:
                return ErrorState(TStringBuilder() << "Unexpected targets: " << Config.GetTargetCase());
        }
    }

    void ProgressTargets(const TActorContext& ctx) {
        for (auto& [_, target] : Targets) {
            target->Progress(PathId.OwnerId, YdbProxy, ctx);
        }
    }

public:
    template <typename T>
    explicit TImpl(ui64 id, const TPathId& pathId, T&& config)
        : ReplicationId(id)
        , PathId(pathId)
        , Config(std::forward<T>(config))
    {
    }

    template <typename... Args>
    ui64 AddTarget(ui64 id, ETargetKind kind, Args&&... args) {
        const auto res = Targets.emplace(id, CreateTarget(id, kind, std::forward<Args>(args)...));
        Y_VERIFY_S(res.second, "Duplicate target: " << id);
        return id;
    }

    template <typename... Args>
    ui64 AddTarget(ETargetKind kind, Args&&... args) {
        return AddTarget(NextTargetId++, kind, std::forward<Args>(args)...);
    }

    ITarget* FindTarget(ui64 id) {
        auto it = Targets.find(id);
        return it != Targets.end()
            ? it->second.Get()
            : nullptr;
    }

    void RemoveTarget(ui64 id) {
        Targets.erase(id);
    }

    void Progress(const TActorContext& ctx) {
        if (!YdbProxy) {
            THolder<IActor> ydbProxy;
            switch (Config.GetCredentialsCase()) {
            case NKikimrReplication::TReplicationConfig::kStaticCredentials:
                ydbProxy.Reset(CreateYdbProxy(Config.GetSrcEndpoint(), Config.GetSrcDatabase(), Config.GetStaticCredentials()));
                break;
            default:
                ErrorState(TStringBuilder() << "Unexpected credentials: " << Config.GetCredentialsCase());
                break;
            }

            if (ydbProxy) {
                YdbProxy = ctx.Register(ydbProxy.Release());
            }
        }

        if (!Tenant && !TenantResolver) {
            TenantResolver = ctx.Register(CreateTenantResolver(ctx.SelfID, ReplicationId, PathId));
        }

        switch (State) {
        case EState::Ready:
            if (!Targets) {
                return DiscoverTargets(ctx);
            } else {
                return ProgressTargets(ctx);
            }
        case EState::Removing:
            if (!Targets) {
                return (void)ctx.Send(ctx.SelfID, new TEvPrivate::TEvDropReplication(ReplicationId));
            } else {
                return ProgressTargets(ctx);
            }
        case EState::Error:
            return;
        }
    }

    void Shutdown(const TActorContext& ctx) {
        for (auto& [_, target] : Targets) {
            target->Shutdown(ctx);
        }

        for (auto& x : TVector<TActorId>{TargetDiscoverer, TenantResolver, YdbProxy}) {
            if (auto actorId = std::exchange(x, {})) {
                ctx.Send(actorId, new TEvents::TEvPoison());
            }
        }
    }

    void SetState(EState state, TString issue = {}) {
        State = state;
        Issue = TruncatedIssue(issue);
    }

    void ErrorState(TString issue) {
        SetState(EState::Error, issue);
    }

private:
    const ui64 ReplicationId;
    const TPathId PathId;
    TString Tenant;

    NKikimrReplication::TReplicationConfig Config;
    EState State = EState::Ready;
    TString Issue;
    ui64 NextTargetId = 1;
    THashMap<ui64, THolder<ITarget>> Targets;
    TActorId YdbProxy;
    TActorId TenantResolver;
    TActorId TargetDiscoverer;

}; // TImpl

TReplication::TReplication(ui64 id, const TPathId& pathId, const NKikimrReplication::TReplicationConfig& config)
    : Impl(std::make_shared<TImpl>(id, pathId, config))
{
}

TReplication::TReplication(ui64 id, const TPathId& pathId, NKikimrReplication::TReplicationConfig&& config)
    : Impl(std::make_shared<TImpl>(id, pathId, std::move(config)))
{
}

static auto ParseConfig(const TString& config) {
    NKikimrReplication::TReplicationConfig cfg;
    Y_VERIFY(cfg.ParseFromString(config));
    return cfg;
}

TReplication::TReplication(ui64 id, const TPathId& pathId, const TString& config)
    : Impl(std::make_shared<TImpl>(id, pathId, ParseConfig(config)))
{
}

ui64 TReplication::AddTarget(ETargetKind kind, const TString& srcPath, const TString& dstPath) {
    return Impl->AddTarget(kind, srcPath, dstPath);
}

TReplication::ITarget* TReplication::AddTarget(ui64 id, ETargetKind kind, const TString& srcPath, const TString& dstPath) {
    Impl->AddTarget(id, kind, srcPath, dstPath);
    return Impl->FindTarget(id);
}

const TReplication::ITarget* TReplication::FindTarget(ui64 id) const {
    return Impl->FindTarget(id);
}

TReplication::ITarget* TReplication::FindTarget(ui64 id) {
    return Impl->FindTarget(id);
}

void TReplication::RemoveTarget(ui64 id) {
    return Impl->RemoveTarget(id);
}

void TReplication::Progress(const TActorContext& ctx) {
    Impl->Progress(ctx);
}

void TReplication::Shutdown(const TActorContext& ctx) {
    Impl->Shutdown(ctx);
}

ui64 TReplication::GetId() const {
    return Impl->ReplicationId;
}

const TPathId& TReplication::GetPathId() const {
    return Impl->PathId;
}

void TReplication::SetState(EState state, TString issue) {
    Impl->SetState(state, issue);
}

TReplication::EState TReplication::GetState() const {
    return Impl->State;
}

const TString& TReplication::GetIssue() const {
    return Impl->Issue;
}

void TReplication::SetNextTargetId(ui64 value) {
    Impl->NextTargetId = value;
}

ui64 TReplication::GetNextTargetId() const {
    return Impl->NextTargetId;
}

void TReplication::SetTenant(const TString& value) {
    Impl->Tenant = value;
    Impl->TenantResolver = {};
}

const TString& TReplication::GetTenant() const {
    return Impl->Tenant;
}

void TReplication::SetDropOp(const TActorId& sender, const std::pair<ui64, ui32>& opId) {
    DropOp = {sender, opId};
}

const std::optional<TReplication::TDropOp>& TReplication::GetDropOp() const {
    return DropOp;
}

}

Y_DECLARE_OUT_SPEC(, NKikimrReplication::TReplicationConfig::TargetCase, stream, value) {
    stream << static_cast<int>(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrReplication::TReplicationConfig::CredentialsCase, stream, value) {
    stream << static_cast<int>(value);
}
