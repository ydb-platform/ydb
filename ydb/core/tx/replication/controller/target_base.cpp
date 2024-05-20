#include "dst_alterer.h"
#include "dst_creator.h"
#include "dst_remover.h"
#include "target_base.h"
#include "util.h"

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NReplication::NController {

using ETargetKind = TReplication::ETargetKind;
using EDstState = TReplication::EDstState;
using EStreamState = TReplication::EStreamState;

TTargetBase::TTargetBase(ETargetKind kind, ui64 id, const TString& srcPath, const TString& dstPath)
    : Id(id)
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

void TTargetBase::Progress(TReplication::TPtr replication, const TActorContext& ctx) {
    switch (DstState) {
    case EDstState::Creating:
        if (!DstCreator) {
            DstCreator = ctx.Register(CreateDstCreator(replication, Id, ctx));
        }
        break;
    case EDstState::Syncing:
        break; // TODO
    case EDstState::Ready:
        if (!WorkerRegistar) {
            WorkerRegistar = ctx.Register(CreateWorkerRegistar(replication, ctx));
        }
        break;
    case EDstState::Alter:
        if (!DstAlterer) {
            DstAlterer = ctx.Register(CreateDstAlterer(replication, Id, ctx));
        }
        break;
    case EDstState::Done:
        break;
    case EDstState::Removing:
        if (!DstRemover) {
            DstRemover = ctx.Register(CreateDstRemover(replication, Id, ctx));
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
