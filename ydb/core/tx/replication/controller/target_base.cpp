#include "dst_creator.h"
#include "target_base.h"
#include "util.h"

#include <library/cpp/actors/core/events.h>

namespace NKikimr::NReplication::NController {

using EDstState = TReplication::EDstState;
using EStreamState = TReplication::EStreamState;

TTargetBase::TTargetBase(ETargetKind kind, ui64 rid, ui64 tid, const TString& srcPath, const TString& dstPath)
    : Kind(kind)
    , ReplicationId(rid)
    , TargetId(tid)
    , SrcPath(srcPath)
    , DstPath(dstPath)
{
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

ui64 TTargetBase::GetReplicationId() const {
    return ReplicationId;
}

ui64 TTargetBase::GetTargetId() const {
    return TargetId;
}

TReplication::ETargetKind TTargetBase::GetTargetKind() const {
    return Kind;
}

void TTargetBase::Progress(ui64 schemeShardId, const TActorId& proxy, const TActorContext& ctx) {
    switch (DstState) {
    case EDstState::Creating:
        if (!DstCreator) {
            DstCreator = ctx.Register(CreateDstCreator(ctx.SelfID, schemeShardId, proxy,
                ReplicationId, TargetId, Kind, SrcPath, DstPath));
        }
        break;
    case EDstState::Syncing:
        break; // TODO
    case EDstState::Ready:
        break; // TODO
    case EDstState::Removing:
        break; // TODO
    case EDstState::Error:
        break;
    }
}

void TTargetBase::Shutdown(const TActorContext& ctx) {
    for (auto& x : TVector<TActorId>{DstCreator, DstRemover}) {
        if (auto actorId = std::exchange(x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }
}

}
