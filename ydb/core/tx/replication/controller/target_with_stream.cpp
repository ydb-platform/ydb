#include "private_events.h"
#include "stream_creator.h"
#include "stream_remover.h"
#include "target_with_stream.h"

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NReplication::NController {

const TString ReplicationConsumerName = "replicationConsumer";

void TTargetWithStream::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Creating:
        if (GetStreamName().empty() && !NameAssignmentInProcess) {
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvAssignStreamName(replication->GetId(), GetId()));
            NameAssignmentInProcess = true;
        } else if (!StreamCreator) {
            StreamCreator = ctx.Register(CreateStreamCreator(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Removing:
        if (GetWorkers()) {
            RemoveWorkers(ctx);
        } else if (!StreamRemover) {
            StreamRemover = ctx.Register(CreateStreamRemover(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Ready:
    case EStreamState::Removed:
    case EStreamState::Error:
        break;
    }

    TTargetBase::Progress(ctx);
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
