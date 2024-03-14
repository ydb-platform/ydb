#include "private_events.h"
#include "stream_creator.h"
#include "stream_remover.h"
#include "target_with_stream.h"

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NReplication::NController {

const TString ReplicationConsumerName = "replicationConsumer";

void TTargetWithStream::Progress(TReplication::TPtr replication, const TActorContext& ctx) {
    const auto& proxy = replication->GetYdbProxy();

    switch (GetStreamState()) {
    case EStreamState::Creating:
        if (GetStreamName().empty() && !NameAssignmentInProcess) {
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvAssignStreamName(GetReplicationId(), GetTargetId()));
            NameAssignmentInProcess = true;
        } else if (!StreamCreator) {
            StreamCreator = ctx.Register(CreateStreamCreator(ctx.SelfID, proxy,
                GetReplicationId(), GetTargetId(), GetTargetKind(), GetSrcPath(), GetStreamName()));
        }
        return;
    case EStreamState::Removing:
        if (!StreamRemover) {
            StreamRemover = ctx.Register(CreateStreamRemover(ctx.SelfID, proxy,
                GetReplicationId(), GetTargetId(), GetTargetKind(), GetSrcPath(), GetStreamName()));
        }
        return;
    case EStreamState::Ready:
    case EStreamState::Removed:
    case EStreamState::Error:
        break;
    }

    TTargetBase::Progress(replication, ctx);
}

void TTargetWithStream::Shutdown(const TActorContext& ctx) {
    for (auto* x : TVector<TActorId*>{&StreamCreator, &StreamRemover}) {
        if (auto actorId = std::exchange(*x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }

    TTargetBase::Shutdown(ctx);
}

}
