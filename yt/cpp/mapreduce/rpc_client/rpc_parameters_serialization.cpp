#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/table_client/name_table.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

NYPath::TRichYPath ToApiRichPath(const TRichYPath& path)
{
    NYPath::TRichYPath richPath;
    auto pathNode = NYson::TYsonString(NodeToYsonString(PathToNode(path), NYson::EYsonFormat::Binary));
    Deserialize(richPath, NYTree::ConvertToNode(pathNode));
    return richPath;
}

TGuid YtGuidFromUtilGuid(TGUID guid)
{
    return {guid.dw[3], guid.dw[2], guid.dw[1], guid.dw[0]};
}

TGUID UtilGuidFromYtGuid(TGuid guid)
{
    return {guid.Parts32[3], guid.Parts32[2], guid.Parts32[1], guid.Parts32[0]};
}

NObjectClient::EObjectType ToApiObjectType(ENodeType type)
{
    switch (type) {
        case NT_STRING:
            return NObjectClient::EObjectType::StringNode;
        case NT_INT64:
            return NObjectClient::EObjectType::Int64Node;
        case NT_UINT64:
            return NObjectClient::EObjectType::Uint64Node;
        case NT_DOUBLE:
            return NObjectClient::EObjectType::DoubleNode;
        case NT_BOOLEAN:
            return NObjectClient::EObjectType::BooleanNode;
        case NT_MAP:
            return NObjectClient::EObjectType::MapNode;
        case NT_LIST:
            THROW_ERROR_EXCEPTION("List nodes are deprecated");
        case NT_FILE:
            return NObjectClient::EObjectType::File;
        case NT_TABLE:
            return NObjectClient::EObjectType::Table;
        case NT_DOCUMENT:
            return NObjectClient::EObjectType::Document;
        case NT_REPLICATED_TABLE:
            return NObjectClient::EObjectType::ReplicatedTable;
        case NT_TABLE_REPLICA:
            return NObjectClient::EObjectType::TableReplica;
        case NT_USER:
            return NObjectClient::EObjectType::User;
        case NT_SCHEDULER_POOL:
            return NObjectClient::EObjectType::SchedulerPool;
        case NT_LINK:
            return NObjectClient::EObjectType::Link;
        case NT_GROUP:
            return NObjectClient::EObjectType::Group;
        case NT_PORTAL:
            return NObjectClient::EObjectType::PortalEntrance;
        case NT_CHAOS_TABLE_REPLICA:
            return NObjectClient::EObjectType::ChaosTableReplica;
        case NT_TABLE_COLLOCATION:
            return NObjectClient::EObjectType::TableCollocation;
    }
    YT_ABORT();
}

NCypressClient::ELockMode ToApiLockMode(ELockMode mode)
{
    switch (mode) {
        case LM_EXCLUSIVE:
            return NCypressClient::ELockMode::Exclusive;
        case LM_SHARED:
            return NCypressClient::ELockMode::Shared;
        case LM_SNAPSHOT:
            return NCypressClient::ELockMode::Snapshot;
    }
    YT_ABORT();
}

NApi::EOperationSortDirection ToApiOperationSortDirection(ECursorDirection direction)
{
    switch (direction) {
        case ECursorDirection::Past:
            return NApi::EOperationSortDirection::Past;
        case ECursorDirection::Future:
            return NApi::EOperationSortDirection::Future;
    }
    YT_ABORT();
}

NYTree::EPermission ToApiPermission(EPermission permission)
{
    switch (permission) {
        case EPermission::Read:
            return NYTree::EPermission::Read;
        case EPermission::Write:
            return NYTree::EPermission::Write;
        case EPermission::Use:
            return NYTree::EPermission::Use;
        case EPermission::Administer:
            return NYTree::EPermission::Administer;
        case EPermission::Create:
            return NYTree::EPermission::Create;
        case EPermission::Remove:
            return NYTree::EPermission::Remove;
        case EPermission::Mount:
            return NYTree::EPermission::Mount;
        case EPermission::Manage:
            return NYTree::EPermission::Manage;
    }
    YT_ABORT();
}

NTransactionClient::EAtomicity ToApiAtomicity(EAtomicity atomicity)
{
    switch (atomicity) {
        case EAtomicity::None:
            return NTransactionClient::EAtomicity::None;
        case EAtomicity::Full:
            return NTransactionClient::EAtomicity::Full;
    }
}

NApi::EJobSortField ToApiJobSortField(EJobSortField field)
{
    switch (field) {
        case EJobSortField::Type:
            return NApi::EJobSortField::Type;
        case EJobSortField::State:
            return NApi::EJobSortField::State;
        case EJobSortField::StartTime:
            return NApi::EJobSortField::StartTime;
        case EJobSortField::FinishTime:
            return NApi::EJobSortField::FinishTime;
        case EJobSortField::Address:
            return NApi::EJobSortField::Address;
        case EJobSortField::Duration:
            return NApi::EJobSortField::Duration;
        case EJobSortField::Progress:
            return NApi::EJobSortField::Progress;
        case EJobSortField::Id:
            return NApi::EJobSortField::Id;
        case EJobSortField::TaskName:
            return NApi::EJobSortField::TaskName;
    }
    YT_ABORT();
}

NApi::EDataSource ToApiDataSource(EListJobsDataSource source)
{
    switch (source) {
        case EListJobsDataSource::Runtime:
            return NApi::EDataSource::Runtime;
        case EListJobsDataSource::Archive:
            return NApi::EDataSource::Archive;
        case EListJobsDataSource::Auto:
            return NApi::EDataSource::Auto;
        case EListJobsDataSource::Manual:
            return NApi::EDataSource::Manual;
    }
    YT_ABORT();
}

NTableClient::ETablePartitionMode ToApiTablePartitionMode(ETablePartitionMode mode)
{
    switch (mode) {
        case ETablePartitionMode::Unordered:
            return NTableClient::ETablePartitionMode::Unordered;
        case ETablePartitionMode::Ordered:
            return NTableClient::ETablePartitionMode::Ordered;
    }
    YT_ABORT();
}

NJobTrackerClient::EJobType ToApiJobType(EJobType type)
{
    switch (type) {
        case EJobType::Map:
            return NJobTrackerClient::EJobType::Map;
        case EJobType::PartitionMap:
            return NJobTrackerClient::EJobType::PartitionMap;
        case EJobType::SortedMerge:
            return NJobTrackerClient::EJobType::SortedMerge;
        case EJobType::OrderedMerge:
            return NJobTrackerClient::EJobType::OrderedMerge;
        case EJobType::UnorderedMerge:
            return NJobTrackerClient::EJobType::UnorderedMerge;
        case EJobType::Partition:
            return NJobTrackerClient::EJobType::Partition;
        case EJobType::SimpleSort:
            return NJobTrackerClient::EJobType::SimpleSort;
        case EJobType::FinalSort:
            return NJobTrackerClient::EJobType::FinalSort;
        case EJobType::SortedReduce:
            return NJobTrackerClient::EJobType::SortedReduce;
        case EJobType::PartitionReduce:
            return NJobTrackerClient::EJobType::PartitionReduce;
        case EJobType::ReduceCombiner:
            return NJobTrackerClient::EJobType::ReduceCombiner;
        case EJobType::RemoteCopy:
            return NJobTrackerClient::EJobType::RemoteCopy;
        case EJobType::IntermediateSort:
            return NJobTrackerClient::EJobType::IntermediateSort;
        case EJobType::OrderedMap:
            return NJobTrackerClient::EJobType::OrderedMap;
        case EJobType::JoinReduce:
            return NJobTrackerClient::EJobType::JoinReduce;
        case EJobType::Vanilla:
            return NJobTrackerClient::EJobType::Vanilla;
        case EJobType::SchedulerUnknown:
            return NJobTrackerClient::EJobType::SchedulerUnknown;
        case EJobType::ReplicateChunk:
            return NJobTrackerClient::EJobType::ReplicateChunk;
        case EJobType::RemoveChunk:
            return NJobTrackerClient::EJobType::RemoveChunk;
        case EJobType::RepairChunk:
            return NJobTrackerClient::EJobType::RepairChunk;
        case EJobType::SealChunk:
            return NJobTrackerClient::EJobType::SealChunk;
        case EJobType::ShallowMerge:
            return NJobTrackerClient::EJobType::ShallowMerge;
        case EJobType::MergeChunks:
            return NJobTrackerClient::EJobType::MergeChunks;
        case EJobType::AutotomizeChunk:
            return NJobTrackerClient::EJobType::AutotomizeChunk;
        case EJobType::ReincarnateChunk:
            return NJobTrackerClient::EJobType::ReincarnateChunk;
        }
    YT_ABORT();
}

NJobTrackerClient::EJobState ToApiJobState(EJobState state)
{
    switch (state) {
        case EJobState::None:
            return NJobTrackerClient::EJobState::None;
        case EJobState::Waiting:
            return NJobTrackerClient::EJobState::Waiting;
        case EJobState::Running:
            return NJobTrackerClient::EJobState::Running;
        case EJobState::Aborting:
            return NJobTrackerClient::EJobState::Aborting;
        case EJobState::Completed:
            return NJobTrackerClient::EJobState::Completed;
        case EJobState::Failed:
            return NJobTrackerClient::EJobState::Failed;
        case EJobState::Aborted:
            return NJobTrackerClient::EJobState::Aborted;
        case EJobState::Lost:
            return NJobTrackerClient::EJobState::Lost;
    }
    YT_ABORT();
}

THashSet<TString> ToApiJobAttributes(const THashSet<EJobAttribute>& attributes) {
    THashSet<TString> result;
    for (const auto& attribute : attributes) {
        result.insert(ToString(attribute));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Generates a new mutation ID based on the given conditions.
// The retry logic in a higher layer resets the mutation ID if the 'useSameMutationId' parameter is set to false.
// If 'useSameMutationId' is true, the function marks the operation for a retry and maintains the same mutation ID.
static void SetMutationId(
    NApi::TMutatingOptions* options,
    TMutationId* mutationId)
{
    if (mutationId->IsEmpty()) {
        CreateGuid(mutationId);
        mutationId->dw[2] = GetPID() ^ MicroSeconds();
    } else {
        options->Retry = true;
    }
    options->MutationId = YtGuidFromUtilGuid(*mutationId);
}

static void SetTransactionId(
    NApi::TTransactionalOptions* options,
    const TTransactionId& transactionId)
{
    options->TransactionId = YtGuidFromUtilGuid(transactionId);
}

template <typename T>
static void SerializeSuppressableAccessTrackingOptions(
    NApi::TSuppressableAccessTrackingOptions* apiOptions,
    const TSuppressableAccessTrackingOptions<T>& options)
{
    apiOptions->SuppressAccessTracking = options.SuppressAccessTracking_;
    apiOptions->SuppressModificationTracking = options.SuppressModificationTracking_;
}

////////////////////////////////////////////////////////////////////////////////

NApi::TGetNodeOptions SerializeOptionsForGet(
    const TTransactionId& transactionId,
    const TGetOptions& options)
{
    NApi::TGetNodeOptions result;
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.AttributeFilter_) {
        result.Attributes = options.AttributeFilter_->Attributes_;
    }
    if (options.MaxSize_) {
        result.MaxSize = *options.MaxSize_;
    }
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TSetNodeOptions SerializeOptionsForSet(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TSetOptions& options)
{
    NApi::TSetNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.Force_) {
        result.Force = *options.Force_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TNodeExistsOptions SerializeOptionsForExists(
    const TTransactionId& transactionId,
    const TExistsOptions& options)
{
    NApi::TNodeExistsOptions result;
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TMultisetAttributesNodeOptions SerializeOptionsForMultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMultisetAttributesOptions& options)
{
    NApi::TMultisetAttributesNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.Force_) {
        result.Force = *options.Force_;
    }
    return result;
}

NApi::TCreateObjectOptions SerializeOptionsForCreateObject(
    TMutationId& mutationId,
    const TCreateOptions& options)
{
    NApi::TCreateObjectOptions result;
    SetMutationId(&result, &mutationId);
    if (options.Attributes_) {
        result.Attributes = NYTree::ConvertToAttributes(
            NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    }
    return result;
}

NApi::TCreateNodeOptions SerializeOptionsForCreate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCreateOptions& options)
{
    NApi::TCreateNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    if (options.Attributes_) {
        result.Attributes = NYTree::ConvertToAttributes(
            NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    }
    result.IgnoreExisting = options.IgnoreExisting_;
    result.IgnoreTypeMismatch = options.IgnoreTypeMismatch_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TCopyNodeOptions SerializeOptionsForCopy(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCopyOptions& options)
{
    NApi::TCopyNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.PreserveAccount = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result.PreserveExpirationTime = *options.PreserveExpirationTime_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TMoveNodeOptions SerializeOptionsForMove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMoveOptions& options)
{
    NApi::TMoveNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.PreserveAccount = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result.PreserveExpirationTime = *options.PreserveExpirationTime_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TRemoveNodeOptions SerializeOptionsForRemove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TRemoveOptions& options)
{
    NApi::TRemoveNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TListNodeOptions SerializeOptionsForList(
    const TTransactionId& transactionId,
    const TListOptions& options)
{
    NApi::TListNodeOptions result;
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.AttributeFilter_) {
        result.Attributes = options.AttributeFilter_->Attributes_;
    }
    if (options.MaxSize_) {
        result.MaxSize = *options.MaxSize_;
    }
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TLinkNodeOptions SerializeOptionsForLink(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLinkOptions& options)
{
    NApi::TLinkNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    if (options.Attributes_) {
        result.Attributes = NYTree::ConvertToAttributes(
            NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    }
    result.IgnoreExisting = options.IgnoreExisting_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TLockNodeOptions SerializeOptionsForLock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLockOptions& options)
{
    NApi::TLockNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Waitable = options.Waitable_;
    if (options.AttributeKey_) {
        result.AttributeKey = *options.AttributeKey_;
    }
    if (options.ChildKey_) {
        result.ChildKey = *options.ChildKey_;
    }
    return result;
}

NApi::TUnlockNodeOptions SerializeOptionsForUnlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TUnlockOptions& /*options*/)
{
    NApi::TUnlockNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    return result;
}

NApi::TConcatenateNodesOptions SerializeOptionsForConcatenate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TConcatenateOptions& /*options*/)
{
    NApi::TConcatenateNodesOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    return result;
}

NApi::TTransactionStartOptions SerializeOptionsForStartTransaction(
    TMutationId& mutationId,
    const TTransactionId& parentId,
    TDuration timeout,
    const TStartTransactionOptions& options)
{
    NApi::TTransactionStartOptions result;
    SetMutationId(&result, &mutationId);
    result.ParentId = YtGuidFromUtilGuid(parentId);
    result.Timeout = options.Timeout_.GetOrElse(timeout);
    if (options.Deadline_) {
        result.Deadline = *options.Deadline_;
    }
    result.PingAncestors = options.PingAncestors_;

    if (options.Attributes_ && !options.Attributes_->IsMap()) {
        ythrow TApiUsageError() << "Attributes must be a Map node";
    }

    auto attributes = NYTree::ConvertToAttributes(
        NYson::TYsonString(NodeToYsonString(options.Attributes_.GetOrElse(TNode::CreateMap()), NYson::EYsonFormat::Binary)));
    if (options.Title_) {
        attributes->Set("title", *options.Title_);
    } else if (!attributes->Contains("title")) {
        attributes->Set("title", GetDefaultTransactionTitle());
    }
    result.Attributes = std::move(attributes);

    return result;
}

NApi::TTransactionAbortOptions SerializeOptionsForAbortTransaction(TMutationId& mutationId)
{
    NApi::TTransactionAbortOptions result;
    SetMutationId(&result, &mutationId);
    return result;
}

NApi::TTransactionCommitOptions SerializeOptionsForCommitTransaction(TMutationId& mutationId)
{
    NApi::TTransactionCommitOptions result;
    SetMutationId(&result, &mutationId);
    return result;
}

NApi::TStartOperationOptions SerializeOptionsForStartOperation(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    NApi::TStartOperationOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    return result;
}

NApi::TGetOperationOptions SerializeOptionsForGetOperation(const TGetOperationOptions& options, bool useAlias)
{
    NApi::TGetOperationOptions result;
    if (options.IncludeRuntime_) {
        result.IncludeRuntime = *options.IncludeRuntime_;
    } else if (useAlias) {
        // Getting operation by operation alias requires enabling this option.
        // So enable it unless user explicitly set it.
        result.IncludeRuntime = true;
    }
    if (options.AttributeFilter_) {
        result.Attributes = THashSet<TString>();
        for (const auto& attribute : options.AttributeFilter_->Attributes_) {
            result.Attributes->emplace(ToString(attribute));
        }
    }
    return result;
}

NApi::TSuspendOperationOptions SerializeOptionsForSuspendOperation(const TSuspendOperationOptions& options)
{
    NApi::TSuspendOperationOptions result;
    if (options.AbortRunningJobs_) {
        result.AbortRunningJobs = *options.AbortRunningJobs_;
    }
    result.Reason = *options.Reason_;
    return result;
}

NApi::TListOperationsOptions SerializeOptionsForListOperations(const TListOperationsOptions& options)
{
    NApi::TListOperationsOptions result;
    if (options.FromTime_) {
        result.FromTime = *options.FromTime_;
    }
    if (options.ToTime_) {
        result.ToTime = *options.ToTime_;
    }
    if (options.CursorTime_) {
        result.CursorTime = *options.CursorTime_;
    }
    if (options.CursorDirection_) {
        result.CursorDirection = ToApiOperationSortDirection(*options.CursorDirection_);
    }
    if (options.Pool_) {
        result.Pool = *options.Pool_;
    }
    if (options.Filter_) {
        result.SubstrFilter = *options.Filter_;
    }
    if (options.User_) {
        result.UserFilter = *options.User_;
    }
    if (options.State_) {
        result.StateFilter = NScheduler::EOperationState(*options.State_);
    }
    if (options.Type_) {
        result.TypeFilter = NScheduler::EOperationType(*options.Type_);
    }
    if (options.WithFailedJobs_) {
        result.WithFailedJobs = *options.WithFailedJobs_;
    }
    if (options.IncludeArchive_) {
        result.IncludeArchive = *options.IncludeArchive_;
    }
    if (options.IncludeCounters_) {
        result.IncludeCounters = *options.IncludeCounters_;
    }
    if (options.Limit_) {
        result.Limit = *options.Limit_;
    }
    return result;
}

NYson::TYsonString SerializeParametersForUpdateOperationParameters(const TUpdateOperationParametersOptions& options)
{
    TNode result;
    if (options.Pool_) {
        result["pool"] = *options.Pool_;
    }
    if (options.Weight_) {
        result["weight"] = *options.Weight_;
    }
    if (options.SchedulingOptionsPerPoolTree_) {
        result["scheduling_options_per_pool_tree"] = TNode::CreateMap();
        for (const auto& [poolTree, schedulingOptions] : options.SchedulingOptionsPerPoolTree_->Options_) {
            auto schedulingOptionsNode = TNode::CreateMap();
            if (schedulingOptions.Pool_) {
                schedulingOptionsNode["pool"] = *schedulingOptions.Pool_;
            }
            if (schedulingOptions.Weight_) {
                schedulingOptionsNode["weight"] = *schedulingOptions.Weight_;
            }
            if (schedulingOptions.ResourceLimits_) {
                auto resourceLimitsNode = TNode::CreateMap();
                const auto& resourceLimits = *schedulingOptions.ResourceLimits_;
                if (resourceLimits.UserSlots_) {
                    resourceLimitsNode["user_slots"] = *resourceLimits.UserSlots_;
                }
                if (resourceLimits.Memory_) {
                    resourceLimitsNode["memory"] = *resourceLimits.Memory_;
                }
                if (resourceLimits.Cpu_) {
                    resourceLimitsNode["cpu"] = *resourceLimits.Cpu_;
                }
                if (resourceLimits.Network_) {
                    resourceLimitsNode["network"] = *resourceLimits.Network_;
                }
                schedulingOptionsNode["resource_limits"] = std::move(resourceLimitsNode);
            }
            result["scheduling_options_per_pool_tree"][poolTree] = std::move(schedulingOptionsNode);
        }
    }
    return NYson::TYsonString(NodeToYsonString(result, NYson::EYsonFormat::Binary));
}

NApi::TGetJobOptions SerializeOptionsForGetJob(const TGetJobOptions& options) {
    NApi::TGetJobOptions result;
    if (options.AttributeFilter_) {
        result.Attributes = ToApiJobAttributes(options.AttributeFilter_->Attributes_);
    }
    return result;
}

NApi::TListJobsOptions SerializeOptionsForListJobs(const TListJobsOptions& options)
{
    NApi::TListJobsOptions result;
    if (options.Type_) {
        result.Type = ToApiJobType(*options.Type_);
    }
    if (options.State_) {
        result.State = ToApiJobState(*options.State_);
    }
    if (options.Address_) {
        result.Address = *options.Address_;
    }
    if (options.WithStderr_) {
        result.WithStderr = *options.WithStderr_;
    }
    if (options.WithSpec_) {
        result.WithSpec = *options.WithSpec_;
    }
    if (options.WithFailContext_) {
        result.WithFailContext = *options.WithFailContext_;
    }
    if (options.WithMonitoringDescriptor_) {
        result.WithMonitoringDescriptor = *options.WithMonitoringDescriptor_;
    }
    if (options.WithInterruptionInfo_) {
        result.WithInterruptionInfo = *options.WithInterruptionInfo_;
    }
    if (options.OperationIncarnation_) {
        result.OperationIncarnation = *options.OperationIncarnation_;
    }
    if (options.MonitoringDescriptor_) {
        result.MonitoringDescriptor = *options.MonitoringDescriptor_;
    }
    if (options.FromTime_) {
        result.FromTime = *options.FromTime_;
    }
    if (options.ToTime_) {
        result.ToTime = *options.ToTime_;
    }
    if (options.ContinuationToken_) {
        result.ContinuationToken = *options.ContinuationToken_;
    }
    if (options.SortField_) {
        result.SortField = ToApiJobSortField(*options.SortField_);
    }
    if (options.SortOrder_) {
        result.SortOrder = NApi::EJobSortDirection(*options.SortOrder_);
    }
    if (options.DataSource_) {
        result.DataSource = ToApiDataSource(*options.DataSource_);
    }
    if (options.IncludeCypress_) {
        result.IncludeCypress = *options.IncludeCypress_;
    }
    if (options.IncludeControllerAgent_) {
        result.IncludeControllerAgent = *options.IncludeControllerAgent_;
    }
    if (options.IncludeArchive_) {
        result.IncludeArchive = *options.IncludeArchive_;
    }
    if (options.Limit_) {
        result.Limit = *options.Limit_;
    }
    if (options.Offset_) {
        result.Offset = *options.Offset_;
    }
    if (options.AttributeFilter_) {
        result.Attributes = ToApiJobAttributes(options.AttributeFilter_->Attributes_);
    }
    return result;
}

NApi::TGetJobTraceOptions SerializeOptionsForGetJobTrace(const TGetJobTraceOptions& options)
{
    NApi::TGetJobTraceOptions result;
    if (options.TraceId_) {
        result.TraceId = NJobTrackerClient::TJobTraceId(YtGuidFromUtilGuid(*options.TraceId_));
    }
    if (options.FromTime_) {
        result.FromTime = *options.FromTime_;
    }
    if (options.ToTime_) {
        result.ToTime = *options.ToTime_;
    }
    return result;
}

NApi::TFileReaderOptions SerializeOptionsForReadFile(
    const TTransactionId& transactionId,
    const TFileReaderOptions& options)
{
    NApi::TFileReaderOptions result;
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    result.Offset = options.Offset_;
    if (options.Length_) {
        result.Length = *options.Length_;
    }
    if (options.Config_) {
        result.Config = ConvertTo<NApi::TFileReaderConfigPtr>(
            NYson::TYsonString(NodeToYsonString(*options.Config_, NYson::EYsonFormat::Binary)));
    }
    return result;
}

NApi::TFileWriterOptions SerializeOptionsForWriteFile(
    const TTransactionId& transactionId,
    const TFileWriterOptions& options)
{
    NApi::TFileWriterOptions result;
    SetTransactionId(&result, transactionId);
    if (options.ComputeMD5_) {
        result.ComputeMD5 = *options.ComputeMD5_;
    }
    result.Config = ConvertTo<NApi::TFileWriterConfigPtr>(
        NYson::TYsonString(NodeToYsonString(*options.Config_.OrElse(TNode::CreateMap()), NYson::EYsonFormat::Binary)));
    if (const auto& writerOptions = options.WriterOptions_) {
        if (writerOptions->EnableEarlyFinish_) {
            result.Config->EnableEarlyFinish = *writerOptions->EnableEarlyFinish_;
        }
        if (writerOptions->UploadReplicationFactor_) {
            result.Config->UploadReplicationFactor = *writerOptions->UploadReplicationFactor_;
        }
        if (writerOptions->MinUploadReplicationFactor_) {
            result.Config->MinUploadReplicationFactor = *writerOptions->UploadReplicationFactor_;
        }
        if (writerOptions->DesiredChunkSize_) {
            result.Config->DesiredChunkSize = *writerOptions->DesiredChunkSize_;
        }
    }
    return result;
}

NApi::TGetFileFromCacheOptions SerializeOptionsForGetFileFromCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& /*options*/)
{
    NApi::TGetFileFromCacheOptions result;
    SetTransactionId(&result, transactionId);
    result.CachePath = cachePath;
    return result;
}

NApi::TPutFileToCacheOptions SerializeOptionsForPutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    NApi::TPutFileToCacheOptions result;
    SetTransactionId(&result, transactionId);
    result.CachePath = cachePath;
    if (options.PreserveExpirationTimeout_) {
        result.PreserveExpirationTimeout = *options.PreserveExpirationTimeout_;
    }
    return result;
}

NApi::TMountTableOptions SerializeOptionsForMountTable(
    TMutationId& mutationId,
    const TMountTableOptions& options)
{
    NApi::TMountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.CellId_) {
        result.CellId = YtGuidFromUtilGuid(*options.CellId_);
    }
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    result.Freeze = options.Freeze_;
    return result;
}

NApi::TUnmountTableOptions SerializeOptionsForUnmountTable(
    TMutationId& mutationId,
    const TUnmountTableOptions& options)
{
    NApi::TUnmountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    result.Force = options.Force_;
    return result;
}

NApi::TRemountTableOptions SerializeOptionsForRemountTable(
    TMutationId& mutationId,
    const TRemountTableOptions& options)
{
    NApi::TRemountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TReshardTableOptions SerializeOptionsForReshardTable(
    TMutationId& mutationId,
    const TReshardTableOptions& options)
{
    NApi::TReshardTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TModifyRowsOptions SerializeOptionsForInsertRows(const TInsertRowsOptions& options)
{
    NApi::TModifyRowsOptions result;
    if (options.RequireSyncReplica_) {
        result.RequireSyncReplica = *options.RequireSyncReplica_;
    }
    if (options.Update_) {
        result.AllowMissingKeyColumns = *options.Update_;
    }
    return result;
}

NApi::TVersionedLookupRowsOptions SerializeOptionsForVersionedLookupRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TLookupRowsOptions& options)
{
    NApi::TVersionedLookupRowsOptions result;
    result.KeepMissingRows = options.KeepMissingRows_;
    if (options.Columns_) {
        const auto& columnNames = options.Columns_->Parts_;

        std::vector<int> indexes;
        indexes.reserve(columnNames.size());
        for (const auto& columnName : columnNames) {
            indexes.push_back(nameTable->GetIdOrThrow(columnName));
        }

        result.ColumnFilter = NTableClient::TColumnFilter(indexes);
    }
    if (options.Timeout_) {
        result.Timeout = *options.Timeout_;
    }
    if (options.Versioned_) {
        NTableClient::TVersionedReadOptions versionedReadOptions;
        versionedReadOptions.ReadMode = NTableClient::EVersionedIOMode::Default;
        result.VersionedReadOptions = std::move(versionedReadOptions);
    }
    return result;
}

NApi::TLookupRowsOptions SerializeOptionsForLookupRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TLookupRowsOptions& options)
{
    NApi::TLookupRowsOptions result;
    result.KeepMissingRows = options.KeepMissingRows_;
    if (options.Columns_) {
        const auto& columnNames = options.Columns_->Parts_;

        std::vector<int> indexes;
        indexes.reserve(columnNames.size());
        for (const auto& columnName : columnNames) {
            indexes.push_back(nameTable->GetIdOrThrow(columnName));
        }

        result.ColumnFilter = NTableClient::TColumnFilter(indexes);
    }
    if (options.Timeout_) {
        result.Timeout = *options.Timeout_;
    }
    if (options.Versioned_) {
        NTableClient::TVersionedReadOptions versionedReadOptions;
        versionedReadOptions.ReadMode = NTableClient::EVersionedIOMode::LatestTimestamp;
        result.VersionedReadOptions = std::move(versionedReadOptions);
    }
    return result;
}

NApi::TSelectRowsOptions SerializeOptionsForSelectRows(const TSelectRowsOptions& options)
{
    NApi::TSelectRowsOptions result;
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.Timeout_) {
        result.Timeout = *options.Timeout_;
    }
    if (options.InputRowLimit_) {
        result.InputRowLimit = *options.InputRowLimit_;
    }
    if (options.OutputRowLimit_) {
        result.OutputRowLimit = *options.OutputRowLimit_;
    }
    result.RangeExpansionLimit = options.RangeExpansionLimit_;
    result.FailOnIncompleteResult = options.FailOnIncompleteResult_;
    result.VerboseLogging = options.VerboseLogging_;
    result.EnableCodeCache = options.EnableCodeCache_;
    return result;
}

NApi::TTableWriterOptions SerializeOptionsForWriteTable(
    const TTransactionId& transactionId,
    const TTableWriterOptions& options)
{
    NApi::TTableWriterOptions result;
    SetTransactionId(&result, transactionId);

    result.Config = ConvertTo<NTableClient::TTableWriterConfigPtr>(
        NYson::TYsonString(NodeToYsonString(*options.Config_.OrElse(TNode::CreateMap()), NYson::EYsonFormat::Binary)));

    if (const auto& writerOptions = options.WriterOptions_) {
        if (writerOptions->EnableEarlyFinish_) {
            result.Config->EnableEarlyFinish = *writerOptions->EnableEarlyFinish_;
        }
        if (writerOptions->UploadReplicationFactor_) {
            result.Config->UploadReplicationFactor = *writerOptions->UploadReplicationFactor_;
        }
        if (writerOptions->MinUploadReplicationFactor_) {
            result.Config->MinUploadReplicationFactor = *writerOptions->MinUploadReplicationFactor_;
        }
        if (writerOptions->DesiredChunkSize_) {
            result.Config->DesiredChunkSize = *writerOptions->DesiredChunkSize_;
        }
    }

    return result;
}

NApi::TTableReaderOptions SerializeOptionsForReadTable(
    const TTransactionId& transactionId,
    const TTableReaderOptions& options)
{
    NApi::TTableReaderOptions result;
    SetTransactionId(&result, transactionId);
    SerializeSuppressableAccessTrackingOptions(&result, options);
    if (options.Config_) {
        result.Config = NYTree::ConvertTo<NTableClient::TTableReaderConfigPtr>(
            NYson::TYsonString(NodeToYsonString(*options.Config_, NYson::EYsonFormat::Binary)));
    }
    result.EnableRowIndex = options.ControlAttributes_.EnableRowIndex_;
    result.EnableRangeIndex = options.ControlAttributes_.EnableRangeIndex_;
    result.OmitInaccessibleRows = options.OmitInaccessibleRows_;
    return result;
}

NApi::TReadTablePartitionOptions SerializeOptionsForReadTablePartition(
    const TTablePartitionReaderOptions& options)
{
    NApi::TReadTablePartitionOptions result;
    SerializeSuppressableAccessTrackingOptions(&result, options);
    result.EnableRowIndex = options.ControlAttributes_.EnableRowIndex_;
    result.EnableRangeIndex = options.ControlAttributes_.EnableRangeIndex_;
    return result;
}

NApi::TAlterTableOptions SerializeOptionsForAlterTable(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TAlterTableOptions& options)
{
    NApi::TAlterTableOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    if (options.Schema_) {
        NTableClient::TTableSchema schema;
        auto schemaNode = NYTree::ConvertToNode(
            NYson::TYsonString(NodeToYsonString(options.Schema_->ToNode(), NYson::EYsonFormat::Binary)));
        Deserialize(schema, schemaNode);
        result.Schema = std::move(schema);
    }
    if (options.Dynamic_) {
        result.Dynamic = *options.Dynamic_;
    }
    if (options.UpstreamReplicaId_) {
        result.UpstreamReplicaId = YtGuidFromUtilGuid(*options.UpstreamReplicaId_);
    }
    return result;
}

NApi::TAlterTableReplicaOptions SerializeOptionsForAlterTableReplica(
    TMutationId& mutationId,
    const TAlterTableReplicaOptions& options)
{
    NApi::TAlterTableReplicaOptions result;
    SetMutationId(&result, &mutationId);
    if (options.Enabled_) {
        result.Enabled = *options.Enabled_;
    }
    if (options.Mode_) {
        result.Mode = NTabletClient::ETableReplicaMode(*options.Mode_);
    }
    return result;
}

NApi::TModifyRowsOptions SerializeOptionsForDeleteRows(const TDeleteRowsOptions& options)
{
    NApi::TModifyRowsOptions result;
    if (options.RequireSyncReplica_) {
        result.RequireSyncReplica = *options.RequireSyncReplica_;
    }
    return result;
}

NApi::TFreezeTableOptions SerializeOptionsForFreezeTable(const TFreezeTableOptions& options)
{
    NApi::TFreezeTableOptions result;
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TUnfreezeTableOptions SerializeOptionsForUnfreezeTable(const TUnfreezeTableOptions& options)
{
    NApi::TUnfreezeTableOptions result;
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TCheckPermissionOptions SerializeOptionsForCheckPermission(const TCheckPermissionOptions& options)
{
    NApi::TCheckPermissionOptions result;
    if (options.Columns_) {
        result.Columns = std::vector<std::string>(options.Columns_.begin(), options.Columns_.end());
    }
    return result;
}

NApi::TGetColumnarStatisticsOptions SerializeOptionsForGetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TGetTableColumnarStatisticsOptions& options)
{
    NApi::TGetColumnarStatisticsOptions result;
    SetTransactionId(&result, transactionId);
    if (options.FetcherMode_) {
        result.FetcherMode = NTableClient::EColumnarStatisticsFetcherMode(*options.FetcherMode_);
    }
    return result;
}

NApi::TPartitionTablesOptions SerializeOptionsForGetTablePartitions(
    const TTransactionId& transactionId,
    const TGetTablePartitionsOptions& options)
{
    NApi::TPartitionTablesOptions result;
    SetTransactionId(&result, transactionId);
    result.PartitionMode = ToApiTablePartitionMode(options.PartitionMode_);
    result.DataWeightPerPartition = options.DataWeightPerPartition_;
    if (options.MaxPartitionCount_) {
        result.MaxPartitionCount = *options.MaxPartitionCount_;
    }
    result.AdjustDataWeightPerPartition = options.AdjustDataWeightPerPartition_;
    result.EnableCookies = options.EnableCookies_;
    return result;
}

NApi::TDistributedWriteSessionStartOptions SerializeOptionsForStartDistributedTableSession(
    TMutationId& /*mutationId*/,
    const TTransactionId& transactionId,
    i64 cookieCount,
    const TStartDistributedWriteTableOptions& options)
{
    NApi::TDistributedWriteSessionStartOptions result;
    SetTransactionId(&result, transactionId);

    result.CookieCount = cookieCount;
    // TODO(achains): Uncomment when TMutatingOptions are supported in native client distributed API.
    // SetMutationId(&result, mutationId);

    if (options.SessionTimeout_) {
        result.SessionTimeout = *options.SessionTimeout_;
    }

    return result;
}

NApi::TDistributedWriteSessionFinishOptions SerializeOptionsForFinishDistributedTableSession(
    TMutationId& /*mutationId*/,
    const TFinishDistributedWriteTableOptions& /*options*/)
{
    NApi::TDistributedWriteSessionFinishOptions result;

    // TODO(achains): Uncomment when TMutatingOptions are supported in native client distributed API.
    // SetMutationId(&result, mutationId);
    return result;
}

NApi::TDistributedWriteFileSessionStartOptions SerializeOptionsForStartDistributedFileSession(
    TMutationId& /*mutationId*/,
    const TTransactionId& transactionId,
    i64 cookieCount,
    const TStartDistributedWriteFileOptions& options)
{
    NApi::TDistributedWriteFileSessionStartOptions result;
    SetTransactionId(&result, transactionId);

    result.CookieCount = cookieCount;
    // TODO(achains): Uncomment when TMutatingOptions are supported in native client distributed API.
    // SetMutationId(&result, mutationId);

    if (options.SessionTimeout_) {
        result.SessionTimeout = *options.SessionTimeout_;
    }

    return result;
}

NApi::TDistributedWriteFileSessionFinishOptions SerializeOptionsForFinishDistributedFileSession(
    TMutationId& /*mutationId*/,
    const TFinishDistributedWriteFileOptions& /*options*/)
{
    NApi::TDistributedWriteFileSessionFinishOptions result;

    // TODO(achains): Uncomment when TMutatingOptions are supported in native client distributed API.
    // SetMutationId(&result, mutationId);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
