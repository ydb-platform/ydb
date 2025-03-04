#include "stream_consumer_remover.h"
#include "target_transfer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NReplication::NController {

TTargetTransfer::TTargetTransfer(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Transfer, id, config)
{
}

void TTargetTransfer::UpdateConfig(const NKikimrReplication::TReplicationConfig& cfg) {
    auto& t = cfg.GetTransferSpecific().GetTargets(0);
    Config = std::make_shared<TTargetTransfer::TTransferConfig>(
        GetConfig()->GetSrcPath(),
        GetConfig()->GetDstPath(),
        t.GetTransformLambda());
}

void TTargetTransfer::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Removing:
        if (GetWorkers()) {
            RemoveWorkers(ctx);
        } else if (!StreamConsumerRemover) {
            StreamConsumerRemover = ctx.Register(CreateStreamConsumerRemover(replication, GetId(), ctx));
        }
        return;
    case EStreamState::Creating:
    case EStreamState::Ready:
    case EStreamState::Removed:
    case EStreamState::Error:
        break;
    }

    TTargetWithStream::Progress(ctx);
}

void TTargetTransfer::Shutdown(const TActorContext& ctx) {
    for (auto* x : TVector<TActorId*>{&StreamConsumerRemover}) {
        if (auto actorId = std::exchange(*x, {})) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }
    }

    TTargetWithStream::Shutdown(ctx);
}

TString TTargetTransfer::GetStreamPath() const {
    return CanonizePath(GetSrcPath());
}

TTargetTransfer::TTransferConfig::TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda)
    : TConfigBase(ETargetKind::Transfer, srcPath, dstPath)
    , TransformLambda(transformLambda)
{
}

const TString& TTargetTransfer::TTransferConfig::GetTransformLambda() const {
    return TransformLambda;
}

}
