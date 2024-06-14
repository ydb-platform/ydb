#include "partition_scale_request.h"

namespace NKikimr {
namespace NPQ {

TPartitionScaleRequest::TPartitionScaleRequest(
    TString topicName,
    TString databasePath,
    ui64 pathId,
    ui64 pathVersion,
    std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit> splits,
    const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge> merges,
    NActors::TActorId parentActorId
)
    : Topic(topicName)
    , DatabasePath(databasePath)
    , PathId(pathId)
    , PathVersion(pathVersion)
    , Splits(splits)
    , Merges(merges)
    , ParentActorId(parentActorId) {

    }

void TPartitionScaleRequest::Bootstrap(const NActors::TActorContext &ctx) {
    SendProposeRequest(ctx);
    Become(&TPartitionScaleRequest::StateWork);
}

void TPartitionScaleRequest::SendProposeRequest(const NActors::TActorContext &ctx) {
    auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    proposal->Record.SetDatabaseName(CanonizePath(DatabasePath));
    FillProposeRequest(*proposal, DatabasePath, Topic, ctx);
    ctx.Send(MakeTxProxyID(), proposal.release());
}

void TPartitionScaleRequest::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TString& workingDir, const TString& topicName, const NActors::TActorContext &ctx) {
    auto& modifyScheme = *proposal.Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
    modifyScheme.SetWorkingDir(workingDir);

    auto applyIf = modifyScheme.AddApplyIf();
    applyIf->SetPathId(PathId);
    applyIf->SetPathVersion(PathVersion == 0 ? 1 : PathVersion);
    applyIf->SetCheckEntityVersion(true);

    NKikimrSchemeOp::TPersQueueGroupDescription groupDescription;
    groupDescription.SetName(topicName);
    TStringBuilder logMessage;
    logMessage << "TPartitionScaleRequest::FillProposeRequest trying to scale partitions. Spilts: ";
    for(const auto& split: Splits) {
        auto* newSplit = groupDescription.AddSplit();
        logMessage << "partition: " << split.GetPartition() << " boundary: '" << split.GetSplitBoundary() << "' ";
        *newSplit = split;
    }
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, logMessage);

    for(const auto& merge: Merges) {
        auto* newMerge = groupDescription.AddMerge();
        *newMerge = merge;
    }

    modifyScheme.MutableAlterPersQueueGroup()->CopyFrom(groupDescription);
}

void TPartitionScaleRequest::PassAway() {
    if (SchemePipeActorId) {
        NTabletPipe::CloseClient(this->SelfId(), SchemePipeActorId);
    }

    TBase::PassAway();
}

void TPartitionScaleRequest::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable);
        Send(ParentActorId, scaleRequestResult.release());
        Die(ctx);
    }
}

void TPartitionScaleRequest::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
    auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable);
    Send(ParentActorId, scaleRequestResult.release());
    Die(ctx);
}

void TPartitionScaleRequest::Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& /*ev*/, const TActorContext& ctx) {
    auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
    Send(ParentActorId, scaleRequestResult.release());
    Die(ctx);
}

void TPartitionScaleRequest::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const NActors::TActorContext& ctx) {
    auto msg = ev->Get();

    auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
    if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
        auto scaleRequestResult = std::make_unique<TEvPartitionScaleRequestDone>(status);
        TStringBuilder issues;
        for (auto& issue : ev->Get()->Record.GetIssues()) {
            issues << issue.ShortDebugString() + ", ";
        }
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "TPartitionScaleRequest "
            << "SchemaShard error when trying to execute a split request: " << issues);
        Send(ParentActorId, scaleRequestResult.release());
        Die(ctx);
    } else {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        if (!SchemePipeActorId) {
            SchemePipeActorId = ctx.ExecutorThread.RegisterActor(NTabletPipe::CreateClient(ctx.SelfID, msg->Record.GetSchemeShardTabletId(), clientConfig));
        }

        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(msg->Record.GetTxId());
        NTabletPipe::SendData(this->SelfId(), SchemePipeActorId, request.release());
    }
}

} // namespace NPQ
} // namespace NKikimr
