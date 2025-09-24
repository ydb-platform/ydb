#include "raw_client.h"

#include "client_impl.h"
#include "raw_batch_request.h"
#include "rpc_parameters_serialization.h"
#include "wrap_rpc_error.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/api/rpc_proxy/client_base.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>
#include <yt/yt/client/api/rpc_proxy/table_writer.h>

#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/iterator/enumerate.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// Workaround until better solution is implemented in YT-26196.
// This timeout is slightly greater than "replication_reader_failure_timeout" in server code.
const TDuration TableReaderTimeout = TDuration::Minutes(11);

////////////////////////////////////////////////////////////////////////////////

ESecurityAction FromApiSecurityAction(NSecurityClient::ESecurityAction action)
{
    switch (action) {
        case NSecurityClient::ESecurityAction::Undefined:
            break;
        case NSecurityClient::ESecurityAction::Allow:
            return ESecurityAction::Allow;
        case NSecurityClient::ESecurityAction::Deny:
            return ESecurityAction::Deny;
    }
    YT_ABORT();
}

TString FromApiOperationState(NScheduler::EOperationState state)
{
    switch (state) {
        case NScheduler::EOperationState::None:
            return "none";
        case NScheduler::EOperationState::Starting:
            return "starting";
        case NScheduler::EOperationState::Orphaned:
            return "orphaned";
        case NScheduler::EOperationState::WaitingForAgent:
            return "waiting_for_agent";
        case NScheduler::EOperationState::Initializing:
            return "initializing";
        case NScheduler::EOperationState::Preparing:
            return "preparing";
        case NScheduler::EOperationState::Materializing:
            return "materializing";
        case NScheduler::EOperationState::ReviveInitializing:
            return "revive_initializing";
        case NScheduler::EOperationState::Reviving:
            return "reviving";
        case NScheduler::EOperationState::RevivingJobs:
            return "reviving_jobs";
        case NScheduler::EOperationState::Pending:
            return "pending";
        case NScheduler::EOperationState::Running:
            return "running";
        case NScheduler::EOperationState::Completing:
            return "completing";
        case NScheduler::EOperationState::Completed:
            return "completed";
        case NScheduler::EOperationState::Aborting:
            return "aborting";
        case NScheduler::EOperationState::Aborted:
            return "aborted";
        case NScheduler::EOperationState::Failing:
            return "failing";
        case NScheduler::EOperationState::Failed:
            return "failed";
    }
    YT_ABORT();
}

EJobType FromApiJobType(NJobTrackerClient::EJobType type)
{
    switch (type) {
        case NJobTrackerClient::EJobType::Map:
            return EJobType::Map;
        case NJobTrackerClient::EJobType::PartitionMap:
            return EJobType::PartitionMap;
        case NJobTrackerClient::EJobType::SortedMerge:
            return EJobType::SortedMerge;
        case NJobTrackerClient::EJobType::OrderedMerge:
            return EJobType::OrderedMerge;
        case NJobTrackerClient::EJobType::UnorderedMerge:
            return EJobType::UnorderedMerge;
        case NJobTrackerClient::EJobType::Partition:
            return EJobType::Partition;
        case NJobTrackerClient::EJobType::SimpleSort:
            return EJobType::SimpleSort;
        case NJobTrackerClient::EJobType::FinalSort:
            return EJobType::FinalSort;
        case NJobTrackerClient::EJobType::SortedReduce:
            return EJobType::SortedReduce;
        case NJobTrackerClient::EJobType::PartitionReduce:
            return EJobType::PartitionReduce;
        case NJobTrackerClient::EJobType::ReduceCombiner:
            return EJobType::ReduceCombiner;
        case NJobTrackerClient::EJobType::RemoteCopy:
            return EJobType::RemoteCopy;
        case NJobTrackerClient::EJobType::IntermediateSort:
            return EJobType::IntermediateSort;
        case NJobTrackerClient::EJobType::OrderedMap:
            return EJobType::OrderedMap;
        case NJobTrackerClient::EJobType::JoinReduce:
            return EJobType::JoinReduce;
        case NJobTrackerClient::EJobType::Vanilla:
            return EJobType::Vanilla;
        case NJobTrackerClient::EJobType::ShallowMerge:
            return EJobType::ShallowMerge;
        case NJobTrackerClient::EJobType::SchedulerUnknown:
            return EJobType::SchedulerUnknown;
        case NJobTrackerClient::EJobType::ReplicateChunk:
            return EJobType::ReplicateChunk;
        case NJobTrackerClient::EJobType::RemoveChunk:
            return EJobType::RemoveChunk;
        case NJobTrackerClient::EJobType::RepairChunk:
            return EJobType::RepairChunk;
        case NJobTrackerClient::EJobType::SealChunk:
            return EJobType::SealChunk;
        case NJobTrackerClient::EJobType::MergeChunks:
            return EJobType::MergeChunks;
        case NJobTrackerClient::EJobType::AutotomizeChunk:
            return EJobType::AutotomizeChunk;
        case NJobTrackerClient::EJobType::ReincarnateChunk:
            return EJobType::ReincarnateChunk;
    }
    YT_ABORT();
}

EJobState FromApiJobState(NJobTrackerClient::EJobState state)
{
    switch (state) {
        case NJobTrackerClient::EJobState::Waiting:
            return EJobState::Waiting;
        case NJobTrackerClient::EJobState::Running:
            return EJobState::Running;
        case NJobTrackerClient::EJobState::Aborting:
            return EJobState::Aborting;
        case NJobTrackerClient::EJobState::Completed:
            return EJobState::Completed;
        case NJobTrackerClient::EJobState::Failed:
            return EJobState::Failed;
        case NJobTrackerClient::EJobState::Aborted:
            return EJobState::Aborted;
        case NJobTrackerClient::EJobState::Lost:
            return EJobState::Lost;
        case NJobTrackerClient::EJobState::None:
            return EJobState::None;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSyncRpcInputStream
    : public IInputStream
{
public:
    explicit TSyncRpcInputStream(std::unique_ptr<IInputStream> stream)
        : Underlying_(std::move(stream))
    { }

private:
    const std::unique_ptr<IInputStream> Underlying_;

    size_t DoRead(void* buf, size_t len) override
    {
        try {
            return Underlying_->Read(buf, len);
        } catch (TErrorException ex) {
            throw ToErrorResponse(std::move(ex));
        }
    }
};

class TSyncRpcOutputStream
    : public IOutputStream
{
public:
    explicit TSyncRpcOutputStream(IAsyncZeroCopyOutputStreamPtr stream)
        : Underlying_(std::move(stream))
    { }

    void DoWrite(const void* buf, size_t len) override
    {
        auto sharedBuffer = TSharedRef::MakeCopy<TDefaultSharedBlobTag>(TRef(buf, len));
        WaitAndProcess(Underlying_->Write(sharedBuffer));
    }

    void DoFinish() override
    {
        WaitAndProcess(Underlying_->Close());
    }

private:
    const IAsyncZeroCopyOutputStreamPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

TRpcRawClient::TRpcRawClient(
    NApi::IClientPtr client,
    const TConfigPtr& config)
    : Client_(std::move(client))
    , Config_(config)
{ }

TNode TRpcRawClient::Get(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->GetNode(newPath, SerializeOptionsForGet(transactionId, options));
    auto result = WaitAndProcess(future);
    return NodeFromYsonString(result.AsStringBuf());
}

TNode TRpcRawClient::TryGet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    try {
        return Get(transactionId, path, options);
    } catch (const TErrorResponse& error) {
        if (!error.IsResolveError()) {
            throw;
        }
        return {};
    }
}

void TRpcRawClient::Set(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto ysonValue = NYson::TYsonString(NodeToYsonString(value, NYson::EYsonFormat::Binary));
    auto future = Client_->SetNode(newPath, ysonValue, SerializeOptionsForSet(mutationId, transactionId, options));
    WaitAndProcess(future);
}

bool TRpcRawClient::Exists(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->NodeExists(newPath, SerializeOptionsForExists(transactionId, options));
    return WaitAndProcess(future);
}

void TRpcRawClient::MultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto attributes = NYTree::ConvertToAttributes(
        NYson::TYsonString(NodeToYsonString(value, NYson::EYsonFormat::Binary)));
    auto future = Client_->MultisetAttributesNode(newPath, attributes->ToMap(), SerializeOptionsForMultisetAttributes(mutationId, transactionId, options));
    WaitAndProcess(future);
}

TNodeId TRpcRawClient::Create(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options)
{
    auto waitGuid = [](auto future) {
        auto result = WaitAndProcess(future);
        return UtilGuidFromYtGuid(result);
    };

    // Call CreateObject on empty path except NT_MAP node
    // With NT_MAP path can be empty, but Config_->Prefix may exist
    if (path.empty() && type != ENodeType::NT_MAP) {
        auto future = Client_->CreateObject(ToApiObjectType(type), SerializeOptionsForCreateObject(mutationId, options));
        return waitGuid(std::move(future));
    }

    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->CreateNode(newPath, ToApiObjectType(type), SerializeOptionsForCreate(mutationId, transactionId, options));
    return waitGuid(std::move(future));
}

TNodeId TRpcRawClient::CopyWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TMutationId mutationId;
    auto newSourcePath = AddPathPrefix(sourcePath, Config_->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Config_->Prefix);
    auto future = Client_->CopyNode(newSourcePath, newDestinationPath, SerializeOptionsForCopy(mutationId, transactionId, options));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::CopyInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    auto newSourcePath = AddPathPrefix(sourcePath, Config_->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Config_->Prefix);

    // Make cross cell copying disable.
    auto newOptions = SerializeOptionsForCopy(mutationId, transactionId, options);
    newOptions.EnableCrossCellCopying = false;

    auto future = Client_->CopyNode(newSourcePath, newDestinationPath, newOptions);
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::MoveWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    TMutationId mutationId;
    auto newSourcePath = AddPathPrefix(sourcePath, Config_->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Config_->Prefix);
    auto future = Client_->MoveNode(newSourcePath, newDestinationPath, SerializeOptionsForMove(mutationId, transactionId, options));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::MoveInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    auto newSourcePath = AddPathPrefix(sourcePath, Config_->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Config_->Prefix);

    // Make cross cell copying disable.
    auto newOptions = SerializeOptionsForMove(mutationId, transactionId, options);
    newOptions.EnableCrossCellCopying = false;

    auto future = Client_->MoveNode(newSourcePath, newDestinationPath, newOptions);
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result);
}

void TRpcRawClient::Remove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->RemoveNode(newPath, SerializeOptionsForRemove(mutationId, transactionId, options));
    WaitAndProcess(future);
}

TNode::TListType TRpcRawClient::List(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    if (path.empty() && newPath.EndsWith('/')) {
        newPath.pop_back();
    }
    auto future = Client_->ListNode(newPath, SerializeOptionsForList(transactionId, options));
    auto result = WaitAndProcess(future);
    return NodeFromYsonString(result.AsStringBuf()).AsList();
}

TNodeId TRpcRawClient::Link(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    auto newTargetPath = AddPathPrefix(targetPath, Config_->Prefix);
    auto newLinkPath = AddPathPrefix(linkPath, Config_->Prefix);
    auto future = Client_->LinkNode(newTargetPath, newLinkPath, SerializeOptionsForLink(mutationId, transactionId, options));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result);
}

TLockId TRpcRawClient::Lock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->LockNode(newPath, ToApiLockMode(mode), SerializeOptionsForLock(mutationId, transactionId, options));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result.LockId);
}

void TRpcRawClient::Unlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->UnlockNode(newPath, SerializeOptionsForUnlock(mutationId, transactionId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::Concatenate(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    std::vector<NYPath::TRichYPath> newSourcePaths;
    for (const auto& sourcePath : sourcePaths) {
        auto newSourcePath = ToApiRichPath(sourcePath);
        newSourcePath.SetPath(AddPathPrefix(newSourcePath.GetPath(), Config_->Prefix));
        newSourcePaths.emplace_back(std::move(newSourcePath));
    }

    auto newDestinationPath = ToApiRichPath(destinationPath);
    newDestinationPath.SetPath(AddPathPrefix(newDestinationPath.GetPath(), Config_->Prefix));
    if (options.Append_) {
        newDestinationPath.SetAppend(*options.Append_);
    }

    TMutationId mutationId;
    auto future = Client_->ConcatenateNodes(newSourcePaths, newDestinationPath, SerializeOptionsForConcatenate(mutationId, transactionId, options));
    WaitAndProcess(future);
}

TTransactionId TRpcRawClient::StartTransaction(
    TMutationId& mutationId,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options)
{
    auto future = Client_->StartTransaction(
        NTransactionClient::ETransactionType::Master,
        SerializeOptionsForStartTransaction(mutationId, parentId, Config_->TxTimeout, options));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result->GetId());
}

void TRpcRawClient::PingTransaction(const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitAndProcess(tx->Ping());
}

void TRpcRawClient::AbortTransaction(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitAndProcess(tx->Abort(SerializeOptionsForAbortTransaction(mutationId)));
}

void TRpcRawClient::CommitTransaction(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitAndProcess(tx->Commit(SerializeOptionsForCommitTransaction(mutationId)));
}

TOperationId TRpcRawClient::StartOperation(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    EOperationType type,
    const TNode& spec)
{
    auto future = Client_->StartOperation(
        NScheduler::EOperationType(type),
        NYson::TYsonString(NodeToYsonString(spec, NYson::EYsonFormat::Binary)),
        SerializeOptionsForStartOperation(mutationId, transactionId));
    auto result = WaitAndProcess(future);
    return UtilGuidFromYtGuid(result.Underlying());
}

TOperationAttributes ParseOperationAttributes(const NApi::TOperation& operation)
{
    TOperationAttributes result;
    if (operation.Id) {
        result.Id = UtilGuidFromYtGuid(operation.Id->Underlying());
    }
    if (operation.Type) {
        result.Type = EOperationType(*operation.Type);
    }
    if (operation.State) {
        result.State = FromApiOperationState(*operation.State);
        if (*result.State == "completed") {
            result.BriefState = EOperationBriefState::Completed;
        } else if (*result.State == "aborted") {
            result.BriefState = EOperationBriefState::Aborted;
        } else if (*result.State == "failed") {
            result.BriefState = EOperationBriefState::Failed;
        } else {
            result.BriefState = EOperationBriefState::InProgress;
        }
    }
    if (operation.AuthenticatedUser) {
        result.AuthenticatedUser = *operation.AuthenticatedUser;
    }
    if (operation.StartTime) {
        result.StartTime = *operation.StartTime;
    }
    if (operation.FinishTime) {
        result.FinishTime = *operation.FinishTime;
    }
    if (operation.BriefProgress) {
        auto briefProgressNode = NodeFromYsonString(operation.BriefProgress.AsStringBuf());
        if (briefProgressNode.HasKey("jobs")) {
            result.BriefProgress.ConstructInPlace();
            const auto& jobs = briefProgressNode["jobs"];
            result.BriefProgress->Aborted = jobs["aborted"].AsInt64();
            result.BriefProgress->Completed = jobs["completed"].AsInt64();
            result.BriefProgress->Running = jobs["running"].AsInt64();
            result.BriefProgress->Total = jobs["total"].AsInt64();
            result.BriefProgress->Failed = jobs["failed"].AsInt64();
            result.BriefProgress->Lost = jobs["lost"].AsInt64();
            result.BriefProgress->Pending = jobs["pending"].AsInt64();
        }
    }
    if (operation.BriefSpec) {
        result.BriefSpec = NodeFromYsonString(operation.BriefSpec.AsStringBuf());
    }
    if (operation.FullSpec) {
        result.FullSpec = NodeFromYsonString(operation.FullSpec.AsStringBuf());
    }
    if (operation.UnrecognizedSpec) {
        result.UnrecognizedSpec = NodeFromYsonString(operation.UnrecognizedSpec.AsStringBuf());
    }
    if (operation.Suspended) {
        result.Suspended = *operation.Suspended;
    }
    if (operation.Result) {
        auto resultNode = NodeFromYsonString(operation.Result.AsStringBuf());
        result.Result.ConstructInPlace();
        auto error = TYtError(resultNode["error"]);
        if (error.GetCode() != 0) {
            result.Result->Error = std::move(error);
        }
    }
    if (operation.Progress) {
        auto progressMap = NodeFromYsonString(operation.Progress.AsStringBuf()).AsMap();
        TMaybe<TInstant> buildTime;
        if (auto buildTimeNode = progressMap.FindPtr("build_time")) {
            buildTime = TInstant::ParseIso8601(buildTimeNode->AsString());
        }
        TJobStatistics jobStatistics;
        if (auto jobStatisticsNode = progressMap.FindPtr("job_statistics")) {
            jobStatistics = TJobStatistics(*jobStatisticsNode);
        }
        TJobCounters jobCounters;
        if (auto jobCountersNode = progressMap.FindPtr("total_job_counter")) {
            jobCounters = TJobCounters(*jobCountersNode);
        }
        result.Progress = TOperationProgress{
            .JobStatistics = std::move(jobStatistics),
            .JobCounters = std::move(jobCounters),
            .BuildTime = buildTime,
        };
    }
    if (operation.Events) {
        auto eventsNode = NodeFromYsonString(operation.Events.AsStringBuf());
        result.Events.ConstructInPlace().reserve(eventsNode.Size());
        for (const auto& eventNode : eventsNode.AsList()) {
            result.Events->push_back(TOperationEvent{
                eventNode["state"].AsString(),
                TInstant::ParseIso8601(eventNode["time"].AsString()),
            });
        }
    }
    if (operation.Alerts) {
        auto alertsNode = NodeFromYsonString(operation.Alerts.AsStringBuf());
        result.Alerts.ConstructInPlace();
        for (const auto& [alertType, alertError] : alertsNode.AsMap()) {
            result.Alerts->emplace(alertType, alertError);
        }
    }
    return result;
}

TOperationAttributes TRpcRawClient::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    auto future = Client_->GetOperation(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForGetOperation(options, /*useAlias*/ false));
    auto result = WaitAndProcess(future);
    return ParseOperationAttributes(result);
}

TOperationAttributes TRpcRawClient::GetOperation(
    const TString& alias,
    const TGetOperationOptions& options)
{
    auto future = Client_->GetOperation(alias, SerializeOptionsForGetOperation(options, /*useAlias*/ true));
    auto result = WaitAndProcess(future);
    return ParseOperationAttributes(result);
}

void TRpcRawClient::AbortOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId)
{
    auto future = Client_->AbortOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitAndProcess(future);
}

void TRpcRawClient::CompleteOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId)
{
    auto future = Client_->CompleteOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitAndProcess(future);
}

void TRpcRawClient::SuspendOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    auto future = Client_->SuspendOperation(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForSuspendOperation(options));
    WaitAndProcess(future);
}

void TRpcRawClient::ResumeOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId,
    const TResumeOperationOptions& /*options*/)
{
    auto future = Client_->ResumeOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitAndProcess(future);
}

TListOperationsResult TRpcRawClient::ListOperations(const TListOperationsOptions& options)
{
    auto future = Client_->ListOperations(SerializeOptionsForListOperations(options));
    auto listOperationsResult = WaitAndProcess(future);

    TListOperationsResult result;
    result.Operations.reserve(listOperationsResult.Operations.size());
    for (const auto& operation : listOperationsResult.Operations) {
        result.Operations.push_back(ParseOperationAttributes(operation));
    }
    if (listOperationsResult.PoolCounts) {
        result.PoolCounts = std::move(*listOperationsResult.PoolCounts);
    }
    if (listOperationsResult.UserCounts) {
        // TODO(babenko): migrate to std::string
        result.UserCounts = {listOperationsResult.UserCounts->begin(), listOperationsResult.UserCounts->end()};
    }
    if (listOperationsResult.StateCounts) {
        result.StateCounts.ConstructInPlace();
        for (const auto& key : TEnumTraits<NScheduler::EOperationState>::GetDomainValues()) {
            (*result.StateCounts)[FromApiOperationState(key)] = (*listOperationsResult.StateCounts)[key];
        }
    }
    if (listOperationsResult.TypeCounts) {
        result.TypeCounts.ConstructInPlace();
        for (const auto& key : TEnumTraits<NScheduler::EOperationType>::GetDomainValues()) {
            (*result.TypeCounts)[EOperationType(key)] = (*listOperationsResult.TypeCounts)[key];
        }
    }
    if (listOperationsResult.FailedJobsCount) {
        result.WithFailedJobsCount = *listOperationsResult.FailedJobsCount;
    }
    result.Incomplete = listOperationsResult.Incomplete;
    return result;
}

void TRpcRawClient::UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    auto future = Client_->UpdateOperationParameters(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeParametersForUpdateOperationParameters(options));
    WaitAndProcess(future);
}

NYson::TYsonString TRpcRawClient::GetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
{
    auto future = Client_->GetJob(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        NJobTrackerClient::TJobId(YtGuidFromUtilGuid(jobId)),
        SerializeOptionsForGetJob(options));
    auto result = WaitAndProcess(future);
    return result;
}

TJobAttributes ParseJobAttributes(const NApi::TJob& job)
{
    TJobAttributes result;
    result.Id = UtilGuidFromYtGuid(job.Id.Underlying());
    if (job.Type) {
        result.Type = FromApiJobType(*job.Type);
    }
    if (auto state = job.GetState()) {
        result.State = FromApiJobState(*state);
    }
    if (job.Address) {
        result.Address = *job.Address;
    }
    if (job.TaskName) {
        result.TaskName = *job.TaskName;
    }
    if (job.StartTime) {
        result.StartTime = *job.StartTime;
    }
    if (job.FinishTime) {
        result.FinishTime = *job.FinishTime;
    }
    if (job.Progress) {
        result.Progress = *job.Progress;
    }
    if (job.StderrSize) {
        result.StderrSize = *job.StderrSize;
    }
    if (job.Error) {
        result.Error.ConstructInPlace(NodeFromYsonString(job.Error.AsStringBuf()));
    }
    if (job.BriefStatistics) {
        result.BriefStatistics = NodeFromYsonString(job.BriefStatistics.AsStringBuf());
    }
    if (job.InputPaths) {
        auto inputPathNodesList = NodeFromYsonString(job.InputPaths.AsStringBuf()).AsList();
        result.InputPaths.ConstructInPlace();
        result.InputPaths->reserve(inputPathNodesList.size());
        for (const auto& inputPathNode : inputPathNodesList) {
            TRichYPath path;
            Deserialize(path, inputPathNode);
            result.InputPaths->emplace_back(std::move(path));
        }
    }
    if (job.CoreInfos) {
        auto coreInfoNodesList = NodeFromYsonString(job.CoreInfos.AsStringBuf()).AsList();
        result.CoreInfos.ConstructInPlace();
        result.CoreInfos->reserve(coreInfoNodesList.size());
        for (const auto& coreInfoNode : coreInfoNodesList) {
            TCoreInfo coreInfo;
            coreInfo.ProcessId = coreInfoNode["process_id"].AsInt64();
            coreInfo.ExecutableName = coreInfoNode["executable_name"].AsString();
            if (coreInfoNode.HasKey("size")) {
                coreInfo.Size = coreInfoNode["size"].AsUint64();
            }
            if (coreInfoNode.HasKey("error")) {
                coreInfo.Error.ConstructInPlace(coreInfoNode["error"]);
            }
            result.CoreInfos->emplace_back(std::move(coreInfo));
        }
    }
    return result;
}

TListJobsResult TRpcRawClient::ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    auto future = Client_->ListJobs(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForListJobs(options));
    auto listJobsResult = WaitAndProcess(future);

    TListJobsResult result;
    result.Jobs.reserve(listJobsResult.Jobs.size());
    for (const auto& job : listJobsResult.Jobs) {
        result.Jobs.push_back(ParseJobAttributes(job));
    }
    if (listJobsResult.CypressJobCount) {
        result.CypressJobCount = *listJobsResult.CypressJobCount;
    }
    if (listJobsResult.ControllerAgentJobCount) {
        result.ControllerAgentJobCount = *listJobsResult.ControllerAgentJobCount;
    }
    if (listJobsResult.ArchiveJobCount) {
        result.ArchiveJobCount = *listJobsResult.ArchiveJobCount;
    }
    return result;
}

class TRpcResponseStream
    : public IFileReader
{
public:
    TRpcResponseStream(std::unique_ptr<IInputStream> stream)
        : Underlying_(std::move(stream))
    { }

private:
    size_t DoRead(void *buf, size_t len) override
    {
        return Underlying_->Read(buf, len);
    }

    size_t DoSkip(size_t len) override
    {
        return Underlying_->Skip(len);
    }

private:
    std::unique_ptr<IInputStream> Underlying_;
};

class TFixedStringStream
    : public IInputStream
{
public:
    TFixedStringStream(TSharedRef data)
        : Data_(data)
    { }

private:
    size_t DoRead(void *buf, size_t len) override
    {
        len = std::min(len, Data_.size() - Position_);
        memcpy(buf, Data_.data() + Position_, len);
        Position_ += len;
        return len;
    }

    size_t DoSkip(size_t len) override
    {
        len = std::min(len, Data_.size() - Position_);
        Position_ += len;
        return len;
    }

private:
    TSharedRef Data_;
    size_t Position_{0};
};

IFileReaderPtr TRpcRawClient::GetJobInput(
    const TJobId& jobId,
    const TGetJobInputOptions& /*options*/)
{
    auto future = Client_->GetJobInput(NJobTrackerClient::TJobId(YtGuidFromUtilGuid(jobId)));
    auto result = WaitAndProcess(future);
    auto stream = std::make_unique<TSyncRpcInputStream>(CreateSyncAdapter(CreateCopyingAdapter(result)));
    return MakeIntrusive<TRpcResponseStream>(std::move(stream));
}

IFileReaderPtr TRpcRawClient::GetJobFailContext(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& /*options*/)
{
    auto future = Client_->GetJobFailContext(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        NJobTrackerClient::TJobId(YtGuidFromUtilGuid(jobId)));
    auto result = WaitAndProcess(future);
    std::unique_ptr<IInputStream> stream(new TFixedStringStream(std::move(result)));
    return MakeIntrusive<TRpcResponseStream>(std::move(stream));
}

IFileReaderPtr TRpcRawClient::GetJobStderr(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /*options*/)
{
    auto future = Client_->GetJobStderr(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        NJobTrackerClient::TJobId(YtGuidFromUtilGuid(jobId)));
    auto result = WaitAndProcess(future);
    std::unique_ptr<IInputStream> stream(new TFixedStringStream(std::move(result.Data)));
    return MakeIntrusive<TRpcResponseStream>(std::move(stream));
}

std::vector<TJobTraceEvent> TRpcRawClient::GetJobTrace(
    const TOperationId& operationId,
    const TGetJobTraceOptions& options)
{
    auto future = Client_->GetJobTrace(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForGetJobTrace(options));
    auto jobTraceEvents = WaitAndProcess(future);

    std::vector<TJobTraceEvent> result;
    result.reserve(jobTraceEvents.size());
    for (const auto& event : jobTraceEvents) {
        result.push_back(TJobTraceEvent{
            .OperationId = UtilGuidFromYtGuid(event.OperationId.Underlying()),
            .JobId = UtilGuidFromYtGuid(event.JobId.Underlying()),
            .TraceId = UtilGuidFromYtGuid(event.TraceId.Underlying()),
            .EventIndex = event.EventIndex,
            .Event = event.Event,
            .EventTime = event.EventTime,
        });
    }
    return result;
}

std::unique_ptr<IInputStream> TRpcRawClient::ReadFile(
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    auto future = Client_->CreateFileReader(path.Path_, SerializeOptionsForReadFile(transactionId, options));
    auto reader = WaitAndProcess(future);
    auto syncAdapter = CreateSyncAdapter(CreateCopyingAdapter(reader));
    return std::make_unique<TSyncRpcInputStream>(std::move(syncAdapter));
}

class TRpcWriteFileRequestStream
    : public IOutputStream
{
public:
    TRpcWriteFileRequestStream(NApi::IFileWriterPtr writer)
        : Writer_(std::move(writer))
    {
        WaitAndProcess(Writer_->Open());
    }

private:
    void DoWrite(const void* buf, size_t len) override
    {
        WaitAndProcess(Writer_->Write(TSharedRef::MakeCopy<TDefaultSharedBlobTag>(TRef(buf, len))));
    }

    void DoFinish() override
    {
        WaitAndProcess(Writer_->Close());
    }

private:
    const NApi::IFileWriterPtr Writer_;
};

std::unique_ptr<IOutputStream> TRpcRawClient::WriteFile(
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto writer = Client_->CreateFileWriter(ToApiRichPath(path), SerializeOptionsForWriteFile(transactionId, options));
    return std::make_unique<TRpcWriteFileRequestStream>(std::move(writer));
}

TMaybe<TYPath> TRpcRawClient::GetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    auto future = Client_->GetFileFromCache(md5Signature, SerializeOptionsForGetFileFromCache(transactionId, cachePath, options));
    auto result = WaitAndProcess(future);
    return result.Path.empty() ? Nothing() : TMaybe<TYPath>(result.Path);
}

TYPath TRpcRawClient::PutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    auto newFilePath = AddPathPrefix(filePath, Config_->Prefix);
    auto future = Client_->PutFileToCache(newFilePath, md5Signature, SerializeOptionsForPutFileToCache(transactionId, cachePath, options));
    auto result = WaitAndProcess(future);
    return result.Path;
}

void TRpcRawClient::MountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TMountTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->MountTable(newPath, SerializeOptionsForMountTable(mutationId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::UnmountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->UnmountTable(newPath, SerializeOptionsForUnmountTable(mutationId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::RemountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TRemountTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->RemountTable(newPath, SerializeOptionsForRemountTable(mutationId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::ReshardTableByPivotKeys(
    TMutationId& mutationId,
    const TYPath& path,
    const TVector<TKey>& keys,
    const TReshardTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);

    std::vector<NTableClient::TLegacyOwningKey> pivotKeys;
    pivotKeys.reserve(keys.size());

    for (const auto& key : keys) {
        auto keysNodesList = TNode::CreateList();
        for (const auto& part : key.Parts_) {
            keysNodesList.Add(part);
        }

        NTableClient::TLegacyOwningKey pivotKey;
        Deserialize(pivotKey, NYTree::ConvertToNode(NYson::TYsonString(
            NodeToYsonString(keysNodesList, NYson::EYsonFormat::Binary))));

        pivotKeys.emplace_back(std::move(pivotKey));
    }

    auto future = Client_->ReshardTable(newPath, pivotKeys,  SerializeOptionsForReshardTable(mutationId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::ReshardTableByTabletCount(
    TMutationId& mutationId,
    const TYPath& path,
    i64 tabletCount,
    const TReshardTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->ReshardTable(newPath, tabletCount, SerializeOptionsForReshardTable(mutationId, options));
    WaitAndProcess(future);
}

NTableClient::TNameTablePtr GetNameTable(const TNode::TListType& rows)
{
    auto nameTable = New<NTableClient::TNameTable>();
    for (const auto& row : rows) {
        for (const auto& [key, _] : row.AsMap()) {
            nameTable->GetIdOrRegisterName(key);
        }
    }
    return nameTable;
}

void TRpcRawClient::InsertRows(
    const TYPath& /* path */,
    const TNode::TListType& /* rows */,
    const TInsertRowsOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("InsertRows is not supported yet via RPC client");
}

void TRpcRawClient::TrimRows(
    const TYPath& /*path*/,
    i64 /*tabletIndex*/,
    i64 /*rowCount*/,
    const TTrimRowsOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("TrimRows is not supported yet via RPC client");
}

TNode::TListType TRpcRawClient::LookupRows(
    const TYPath& /*path*/,
    const TNode::TListType& /*keys*/,
    const TLookupRowsOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("LookupRows is not supported yet via RPC client");
}

TNode::TListType TRpcRawClient::SelectRows(
    const TString& /*query*/,
    const TSelectRowsOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("SelectRows is not supported yet via RPC client");
}

void TRpcRawClient::AlterTable(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options)
{
    auto future = Client_->AlterTable(path, SerializeOptionsForAlterTable(mutationId, transactionId, options));
    WaitAndProcess(future);
}

class TDeserializingRowStream
    : public IAsyncZeroCopyInputStream
{
public:
    explicit TDeserializingRowStream(IAsyncZeroCopyInputStreamPtr stream)
        : Underlying_(std::move(stream))
    { }

    TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read().Apply(BIND([=] (const TSharedRef& block) {
            NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
            return NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);
        }));
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
};

class TSerializingRowStream
    : public IAsyncZeroCopyOutputStream
{
public:
    explicit TSerializingRowStream(IAsyncZeroCopyOutputStreamPtr stream)
        : Underlying_(std::move(stream))
    { }

    TFuture<void> Write(const TSharedRef& data) override
    {
        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        descriptor.set_rowset_format(NApi::NRpcProxy::NProto::RF_FORMAT);

        auto [block, payloadRef] = NApi::NRpcProxy::SerializeRowStreamBlockEnvelope(
            data.Size(),
            descriptor,
            nullptr);

        std::copy(data.Begin(), data.End(), payloadRef.Begin());
        return Underlying_->Write(block);
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

private:
    const IAsyncZeroCopyOutputStreamPtr Underlying_;
};

std::unique_ptr<IOutputStream> TRpcRawClient::WriteTable(
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TMaybe<TFormat>& format,
    const TTableWriterOptions& options)
{
    auto* clientBase = VerifyDynamicCast<NApi::NRpcProxy::TClientBase*>(Client_.Get());

    auto apiOptions = SerializeOptionsForWriteTable(transactionId, options);

    auto proxy = clientBase->CreateApiServiceProxy();

    auto req = proxy.WriteTable();
    clientBase->InitStreamingRequest(*req);

    auto apiPath = ToApiRichPath(path);

    ToProto(req->mutable_path(), apiPath);

    if (apiOptions.Config) {
        req->set_config(NYson::ConvertToYsonString(*apiOptions.Config).ToString());
    }

    if (format) {
        req->set_format(NYson::TYsonString(NodeToYsonString(format->Config, NYson::EYsonFormat::Text)).ToString());
    }

    ToProto(req->mutable_transactional_options(), apiOptions);

    auto future = NRpc::CreateRpcClientOutputStream(
        std::move(req), BIND ([=](const TSharedRef& metaRef) {
            NApi::NRpcProxy::NProto::TWriteTableMeta meta;
            if (!TryDeserializeProto(&meta, metaRef)) {
                THROW_ERROR_EXCEPTION("Failed to deserialize schema for table writer");
            }
    }));

    auto stream = WaitAndProcess(future);
    auto rowStream = New<TSerializingRowStream>(std::move(stream));
    return std::make_unique<TSyncRpcOutputStream>(std::move(rowStream));
}

std::unique_ptr<IInputStream> TRpcRawClient::ReadTable(
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TMaybe<TFormat>& format,
    const TTableReaderOptions& options)
{
    auto* clientBase = VerifyDynamicCast<NApi::NRpcProxy::TClientBase*>(Client_.Get());

    auto apiOptions = SerializeOptionsForReadTable(transactionId, options);

    auto proxy = clientBase->CreateApiServiceProxy();

    auto req = proxy.ReadTable();
    clientBase->InitStreamingRequest(*req);
    req->ClientAttachmentsStreamingParameters().ReadTimeout = TableReaderTimeout;
    req->ClientAttachmentsStreamingParameters().WriteTimeout = TableReaderTimeout;

    ToProto(req->mutable_path(), ToApiRichPath(path));

    req->set_unordered(apiOptions.Unordered);
    req->set_omit_inaccessible_columns(apiOptions.OmitInaccessibleColumns);
    req->set_enable_table_index(apiOptions.EnableTableIndex);
    req->set_enable_row_index(apiOptions.EnableRowIndex);
    req->set_enable_range_index(apiOptions.EnableRangeIndex);
    req->set_enable_any_unpacking(apiOptions.EnableAnyUnpacking);

    if (apiOptions.Config) {
        req->set_config(NYson::ConvertToYsonString(*apiOptions.Config).ToString());
    }

    if (format) {
        req->set_desired_rowset_format(NApi::NRpcProxy::NProto::RF_FORMAT);
        req->set_format(NYson::TYsonString(NodeToYsonString(format->Config, NYson::EYsonFormat::Text)).ToString());
    }

    ToProto(req->mutable_transactional_options(), apiOptions);
    ToProto(req->mutable_suppressable_access_tracking_options(), apiOptions);

    auto future = NRpc::CreateRpcClientInputStream(std::move(req));
    auto stream = WaitAndProcess(future);

    auto metaRef = WaitAndProcess(stream->Read());

    NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    auto rowStream = New<TDeserializingRowStream>(std::move(stream));
    auto syncAdapter = CreateSyncAdapter(CreateCopyingAdapter(std::move(rowStream)));
    return std::make_unique<TSyncRpcInputStream>(std::move(syncAdapter));
}

std::unique_ptr<IInputStream> TRpcRawClient::ReadTablePartition(
    const TString& cookie,
    const TMaybe<TFormat>& format,
    const TTablePartitionReaderOptions& options)
{
    auto* clientBase = VerifyDynamicCast<NApi::NRpcProxy::TClientBase*>(Client_.Get());

    auto proxy = clientBase->CreateApiServiceProxy();

    auto req = proxy.ReadTablePartition();
    clientBase->InitStreamingRequest(*req);

    req->set_cookie(cookie);

    auto apiOptions = SerializeOptionsForReadTablePartition(options);

    if (format) {
        req->set_desired_rowset_format(NApi::NRpcProxy::NProto::RF_FORMAT);
        req->set_format(NYson::TYsonString(NodeToYsonString(format->Config, NYson::EYsonFormat::Text)).ToString());
    }

    auto future = NRpc::CreateRpcClientInputStream(std::move(req));
    auto stream = WaitAndProcess(future);

    auto metaRef = WaitAndProcess(stream->Read());

    NApi::NRpcProxy::NProto::TRspReadTablePartitionMeta meta;
    if (!TryDeserializeProto(&meta, metaRef)) {
        THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
    }

    auto rowStream = New<TDeserializingRowStream>(std::move(stream));
    auto syncAdapter = CreateSyncAdapter(CreateCopyingAdapter(std::move(rowStream)));
    return std::make_unique<TSyncRpcInputStream>(std::move(syncAdapter));
}

std::unique_ptr<IInputStream> TRpcRawClient::ReadBlobTable(
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TKey& key,
    const TBlobTableReaderOptions& options)
{
    auto lowerKeyNode = TNode::CreateList(key.Parts_);
    lowerKeyNode.Add(options.StartPartIndex_);

    auto lowerLimitKeyNode = TNode::CreateList();
    lowerLimitKeyNode.Add(">=");
    lowerLimitKeyNode.Add(lowerKeyNode);

    NTableClient::TOwningKeyBound lowerKeyBound;
    Deserialize(lowerKeyBound, NYTree::ConvertToNode(NYson::TYsonString(
        NodeToYsonString(lowerLimitKeyNode, NYson::EYsonFormat::Binary))));

    auto upperKeyNode = TNode::CreateList(key.Parts_);
    upperKeyNode.Add(std::numeric_limits<i64>::max());

    auto upperLimitKeyNode = TNode::CreateList();
    upperLimitKeyNode.Add("<");
    upperLimitKeyNode.Add(upperKeyNode);

    NTableClient::TOwningKeyBound upperKeyBound;
    Deserialize(upperKeyBound, NYTree::ConvertToNode(NYson::TYsonString(
        NodeToYsonString(upperLimitKeyNode, NYson::EYsonFormat::Binary))));

    auto richPath = ToApiRichPath(path);
    richPath.SetRanges({
        NChunkClient::TReadRange{
            NChunkClient::TReadLimit{lowerKeyBound},
            NChunkClient::TReadLimit{upperKeyBound}}});

    auto future = Client_->CreateTableReader(richPath, SerializeOptionsForReadTable(transactionId));
    auto reader = WaitAndProcess(future);

    std::optional<std::string> partIndexColumnName;
    if (options.PartIndexColumnName_) {
        partIndexColumnName = *options.PartIndexColumnName_;
    }
    std::optional<std::string> dataColumnName;
    if (options.DataColumnName_) {
        dataColumnName = *options.DataColumnName_;
    }

    auto blobReader = NTableClient::CreateBlobTableReader(
        reader,
        partIndexColumnName,
        dataColumnName,
        options.StartPartIndex_,
        options.Offset_,
        options.PartSize_);
    auto syncAdapter = CreateSyncAdapter(CreateCopyingAdapter(blobReader));
    return std::make_unique<TSyncRpcInputStream>(std::move(syncAdapter));
}

void TRpcRawClient::AlterTableReplica(
    TMutationId& mutationId,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options)
{
    auto future = Client_->AlterTableReplica(
        YtGuidFromUtilGuid(replicaId),
        SerializeOptionsForAlterTableReplica(mutationId, options));
    WaitAndProcess(future);
}

void TRpcRawClient::DeleteRows(
    const TYPath& /*path*/,
    const TNode::TListType& /*keys*/,
    const TDeleteRowsOptions& /*options*/)
{
    THROW_ERROR_EXCEPTION("DeleteRows is not supported yet via RPC client");
}

void TRpcRawClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->FreezeTable(newPath, SerializeOptionsForFreezeTable(options));
    WaitAndProcess(future);
}

void TRpcRawClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->UnfreezeTable(newPath, SerializeOptionsForUnfreezeTable(options));
    WaitAndProcess(future);
}

TCheckPermissionResponse ParseCheckPermissionResponse(const NApi::TCheckPermissionResponse& response)
{
    auto parseSingleResult = [] (const NApi::TCheckPermissionResult& result) {
        TCheckPermissionResult parsed;
        parsed.Action = FromApiSecurityAction(result.Action);
        parsed.ObjectId = UtilGuidFromYtGuid(result.ObjectId);
        if (result.ObjectName) {
            parsed.ObjectName = *result.ObjectName;
        }
        parsed.SubjectId = UtilGuidFromYtGuid(result.SubjectId);
        if (result.SubjectName) {
            parsed.SubjectName = *result.SubjectName;
        }
        return parsed;
    };

    TCheckPermissionResponse result;
    static_cast<TCheckPermissionResult&>(result) = parseSingleResult(response);
    if (auto columns = response.Columns) {
        result.Columns.reserve(columns->size());
        for (const auto& column : *columns) {
            result.Columns.push_back(parseSingleResult(column));
        }
    }
    return result;
}

TCheckPermissionResponse TRpcRawClient::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->CheckPermission(user, newPath, ToApiPermission(permission), SerializeOptionsForCheckPermission(options));
    auto result = WaitAndProcess(future);
    return ParseCheckPermissionResponse(result);
}

TVector<TTabletInfo> TRpcRawClient::GetTabletInfos(
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& /*options*/)
{
    auto newPath = AddPathPrefix(path, Config_->Prefix);
    auto future = Client_->GetTabletInfos(newPath, tabletIndexes);
    auto tabletInfos = WaitAndProcess(future);

    TVector<TTabletInfo> result;
    result.reserve(tabletInfos.size());
    for (const auto& info : tabletInfos) {
        result.push_back(TTabletInfo{
            .TotalRowCount = info.TotalRowCount,
            .TrimmedRowCount = info.TrimmedRowCount,
            .BarrierTimestamp = info.BarrierTimestamp,
        });
    }
    return result;
}

TVector<TTableColumnarStatistics> TRpcRawClient::GetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    std::vector<NYPath::TRichYPath> newPaths(paths.size());
    std::transform(paths.begin(), paths.end(), newPaths.begin(), ToApiRichPath);

    auto future = Client_->GetColumnarStatistics(newPaths, SerializeOptionsForGetTableColumnarStatistics(transactionId, options));
    auto tableColumnarStatistics = WaitAndProcess(future);

    YT_VERIFY(newPaths.size() == tableColumnarStatistics.size());
    for (int index = 0; index < std::ssize(tableColumnarStatistics); ++index) {
        YT_VERIFY(std::ssize(*newPaths[index].GetColumns()) == tableColumnarStatistics[index].GetColumnCount());
    }

    TVector<TTableColumnarStatistics> result;
    result.reserve(tableColumnarStatistics.size());

    for (const auto& [tableIdx, entry] : Enumerate(tableColumnarStatistics)) {
        TTableColumnarStatistics statistics;
        if (auto columns = newPaths[tableIdx].GetColumns()) {
            for (const auto& [columnIdx, columnName] : Enumerate(*columns)) {
                statistics.ColumnDataWeight[columnName] = entry.ColumnDataWeights[columnIdx];
                if (entry.HasLargeStatistics()) {
                    statistics.ColumnEstimatedUniqueCounts[columnName] = entry.LargeStatistics.ColumnHyperLogLogDigests[columnIdx].EstimateCardinality();
                }
            }
        }

        statistics.LegacyChunksDataWeight = entry.LegacyChunkDataWeight;
        if (entry.TimestampTotalWeight) {
            statistics.TimestampTotalWeight = *entry.TimestampTotalWeight;
        }

        result.emplace_back(std::move(statistics));
    }
    return result;
}

TMultiTablePartitions TRpcRawClient::GetTablePartitions(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    std::vector<NYPath::TRichYPath> newPaths(paths.size());
    std::transform(paths.begin(), paths.end(), newPaths.begin(), ToApiRichPath);

    auto future = Client_->PartitionTables(newPaths, SerializeOptionsForGetTablePartitions(transactionId, options));
    auto multiTablePartitions = WaitAndProcess(future);

    TMultiTablePartitions result;
    result.Partitions.reserve(multiTablePartitions.Partitions.size());

    for (const auto& entry : multiTablePartitions.Partitions) {
        TMultiTablePartition partition;
        partition.TableRanges.reserve(entry.TableRanges.size());

        for (const auto& tableRange : entry.TableRanges) {
            TNode tableRangeNode;
            TNodeBuilder builder(&tableRangeNode);
            Serialize(tableRange, &builder);

            TRichYPath actualTableRange;
            Deserialize(actualTableRange, tableRangeNode);
            partition.TableRanges.emplace_back(std::move(actualTableRange));
        }

        const auto& statistics = entry.AggregateStatistics;
        partition.AggregateStatistics = TMultiTablePartition::TStatistics{
            .ChunkCount = statistics.ChunkCount,
            .DataWeight = statistics.DataWeight,
            .RowCount = statistics.RowCount,
        };

        result.Partitions.emplace_back(std::move(partition));
    }
    return result;
}

ui64 TRpcRawClient::GenerateTimestamp()
{
    auto future = Client_->GetTimestampProvider()->GenerateTimestamps();
    auto result = WaitAndProcess(future);
    return result;
}

IRawBatchRequestPtr TRpcRawClient::CreateRawBatchRequest()
{
    return MakeIntrusive<TRpcRawBatchRequest>(Clone(), Config_);
}

IRawClientPtr TRpcRawClient::Clone()
{
    return ::MakeIntrusive<TRpcRawClient>(Client_, Config_);
}

IRawClientPtr TRpcRawClient::Clone(const TClientContext& context)
{
    return ::MakeIntrusive<TRpcRawClient>(CreateApiClient(context), context.Config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
