#include "dst_alterer.h"
#include "dst_creator.h"
#include "dst_remover.h"
#include "private_events.h"
#include "target_base.h"
#include "util.h"

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NReplication::NController {

using ETargetKind = TReplication::ETargetKind;
using EDstState = TReplication::EDstState;
using EStreamState = TReplication::EStreamState;

TTargetBase::TConfigBase::TConfigBase(ETargetKind kind, const TString& srcPath, const TString& dstPath)
    : Kind(kind)
    , SrcPath(srcPath)
    , DstPath(dstPath)
{
}

ETargetKind TTargetBase::TConfigBase::GetKind() const {
    return Kind;
}

const TString& TTargetBase::TConfigBase::GetSrcPath() const {
    return SrcPath;
}

const TString& TTargetBase::TConfigBase::GetDstPath() const {
    return DstPath;
}

TTargetBase::TTargetBase(TReplication* replication, ETargetKind kind,
        ui64 id, const IConfig::TPtr& config)
    : Replication(replication)
    , Id(id)
    , Kind(kind)
    , Config(config)
{
    Y_ABORT_UNLESS(kind == config->GetKind());
}

ui64 TTargetBase::GetId() const {
    return Id;
}

ETargetKind TTargetBase::GetKind() const {
    return Kind;
}

const TReplication::ITarget::IConfig::TPtr& TTargetBase::GetConfig() const {
    return Config;
}

const TString& TTargetBase::GetSrcPath() const {
    return Config->GetSrcPath();
}

const TString& TTargetBase::GetDstPath() const {
    return Config->GetDstPath();
}

EDstState TTargetBase::GetDstState() const {
    return DstState;
}

void TTargetBase::SetDstState(const EDstState value) {
    DstState = value;
    switch (DstState) {
    case EDstState::Alter:
        Replication->AddPendingAlterTarget(Id);
        break;
    default:
        Replication->RemovePendingAlterTarget(Id);
        break;
    }

    if (DstState != EDstState::Creating) {
        Reset(DstCreator);
    }
    if (DstState != EDstState::Ready) {
        Reset(WorkerRegistar);
    }
    if (DstState != EDstState::Alter) {
        Reset(DstAlterer);
    }
    if (DstState != EDstState::Removing) {
        Reset(DstRemover);
    }
    if (DstState != EDstState::Alter && DstState != EDstState::Removing) {
        PendingRemoveWorkers = false;
    }
}

const TPathId& TTargetBase::GetDstPathId() const {
    return DstPathId;
}

void TTargetBase::SetDstPathId(const TPathId& value) {
    DstPathId = value;
}

const TString& TTargetBase::GetStreamName() const {
    return StreamName;
}

void TTargetBase::SetStreamName(const TString& value) {
    StreamName = value;
}

const TString& TTargetBase::GetStreamConsumerName() const {
    return StreamConsumerName;
}

void TTargetBase::SetStreamConsumerName(const TString& value) {
    StreamConsumerName = value;
}

EStreamState TTargetBase::GetStreamState() const {
    return StreamState;
}

void TTargetBase::SetStreamState(EStreamState value) {
    StreamState = value;
}

const TString& TTargetBase::GetIssue() const {
    return Issue;
}

void TTargetBase::SetIssue(const TString& value) {
    Issue = value;
    TruncatedIssue(Issue);
}

void TTargetBase::AddWorker(ui64 id) {
    Workers.emplace(id, TWorker{});
    TLagProvider::AddPendingLag(id);
}

void TTargetBase::RemoveWorker(ui64 id) {
    Workers.erase(id);
}

TVector<ui64> TTargetBase::GetWorkers() const {
    TVector<ui64> result(::Reserve(Workers.size()));
    for (const auto& [id, _] : Workers) {
        result.push_back(id);
    }
    return result;
}

bool TTargetBase::HasWorkers() const {
    return !Workers.empty();
}

void TTargetBase::RemoveWorkers(const TActorContext& ctx) {
    if (!PendingRemoveWorkers) {
        PendingRemoveWorkers = true;
        for (const auto& [id, _] : Workers) {
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvRemoveWorker(Replication->GetId(), Id, id));
        }
    }
}

void TTargetBase::UpdateLag(ui64 workerId, TDuration lag) {
    auto it = Workers.find(workerId);
    if (it == Workers.end()) {
        return;
    }

    if (TLagProvider::UpdateLag(it->second, workerId, lag)) {
        Replication->UpdateLag(GetId(), TLagProvider::GetLag().GetRef());
    }
}

const TMaybe<TDuration> TTargetBase::GetLag() const {
    return TLagProvider::GetLag();
}

void TTargetBase::Progress(const TActorContext& ctx) {
    switch (DstState) {
    case EDstState::Creating:
        if (!DstCreator) {
            DstCreator = ctx.Register(CreateDstCreator(Replication, Id, ctx));
        }
        break;
    case EDstState::Ready:
        if (!WorkerRegistar) {
            WorkerRegistar = ctx.Register(CreateWorkerRegistar(ctx));
        }
        break;
    case EDstState::Alter:
        if (Workers) {
            RemoveWorkers(ctx);
        } else if (!DstCreator && !DstPathId) {
            DstCreator = ctx.Register(CreateDstCreator(Replication, Id, ctx));
        } else if (!DstAlterer) {
            DstAlterer = ctx.Register(CreateDstAlterer(Replication, Id, ctx));
        }
        break;
    case EDstState::Done:
        break;
    case EDstState::Removing:
        if (Workers) {
            RemoveWorkers(ctx);
        } else if (!DstRemover) {
            DstRemover = ctx.Register(CreateDstRemover(Replication, Id, ctx));
        }
        break;
    case EDstState::Paused:
        break;
    case EDstState::Error:
        break;
    }
}

void TTargetBase::Shutdown(const TActorContext& ctx) {
    TVector<TActorId*> toShutdown = {
        &DstCreator,
        &DstAlterer,
        &DstRemover,
        &WorkerRegistar,
    };

    for (auto* x : toShutdown) {
        if (auto actorId = std::exchange(*x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }
}

void TTargetBase::Reset(TActorId& id) {
    if (auto actorId = std::exchange(id, {})) {
        TlsActivationContext->AsActorContext().Send(actorId, new TEvents::TEvPoison());
    }
}

void TTargetBase::UpdateConfig(const NKikimrReplication::TReplicationConfig&) {
}

}
