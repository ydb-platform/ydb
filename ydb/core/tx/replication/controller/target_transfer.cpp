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
    Config = std::make_shared<TTargetTransfer::TTransferConfig>(
        GetConfig()->GetSrcPath(),
        GetConfig()->GetDstPath(),
        cfg);
}

void TTargetTransfer::Progress(const TActorContext& ctx) {
    auto replication = GetReplication();

    switch (GetStreamState()) {
    case EStreamState::Removing:
        if (HasWorkers()) {
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

TTargetTransfer::TTransferConfig::TTransferConfig(const TString& srcPath, const TString& dstPath, const NKikimrReplication::TReplicationConfig& cfg)
    : TConfigBase(ETargetKind::Transfer, srcPath, dstPath)
    , TransformLambda(cfg.GetTransferSpecific().GetTarget().GetTransformLambda())
    , RunAsUser(cfg.GetTransferSpecific().GetRunAsUser())
    , DirectoryPath(cfg.GetTransferSpecific().GetTarget().GetDirectoryPath())
{
}

const TString& TTargetTransfer::TTransferConfig::GetTransformLambda() const {
    return TransformLambda;
}

const TString& TTargetTransfer::TTransferConfig::GetRunAsUser() const {
    return RunAsUser;
}

const TString& TTargetTransfer::TTransferConfig::GetDirectoryPath() const {
    return DirectoryPath;
}

}
