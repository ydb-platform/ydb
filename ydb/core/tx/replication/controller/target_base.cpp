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

TTargetBase::TTargetBase(TReplication* replication, ETargetKind kind,
        ui64 id, const TString& srcPath, const TString& dstPath)
    : Replication(replication)
    , Id(id)
    , Kind(kind)
    , SrcPath(srcPath)
    , DstPath(dstPath)
{
}

ui64 TTargetBase::GetId() const {
    return Id;
}

ETargetKind TTargetBase::GetKind() const {
    return Kind;
}

const TString& TTargetBase::GetSrcPath() const {
    return SrcPath;
}

const TString& TTargetBase::GetDstPath() const {
    return DstPath;
}

EDstState TTargetBase::GetDstState() const {
    return DstState;
}

void TTargetBase::SetDstState(const EDstState value) {
    DstState = value;
    switch (DstState) {
    case EDstState::Alter:
        return Replication->AddPendingAlterTarget(Id);
    case EDstState::Done:
        return Replication->RemovePendingAlterTarget(Id);
    default:
        break;
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

const THashMap<ui64, TTargetBase::TWorker>& TTargetBase::GetWorkers() const {
    return Workers;
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

}
