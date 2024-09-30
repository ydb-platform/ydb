#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>

namespace NYT::NDetail::NRawClient {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static void SetTransactionIdParam(TNode* node, const TTransactionId& transactionId)
{
    if (transactionId != TTransactionId()) {
        (*node)["transaction_id"] = GetGuidAsString(transactionId);
    }
}

static void SetOperationIdParam(TNode* node, const TOperationId& operationId)
{
    (*node)["operation_id"] = GetGuidAsString(operationId);
}

static void SetAliasParam(TNode* node, const TString& alias)
{
    (*node)["operation_alias"] = alias;
}

static void SetPathParam(TNode* node, const TString& pathPrefix, const TYPath& path)
{
    TYPath updatedPath = AddPathPrefix(path, pathPrefix);
    // Translate "//" to "/"
    // Translate "//some/constom/prefix/from/config/" to "//some/constom/prefix/from/config"
    if (path.empty() && updatedPath.EndsWith('/')) {
        updatedPath.pop_back();
    }
    (*node)["path"] = std::move(updatedPath);
}

static TNode SerializeAttributeFilter(const TAttributeFilter& attributeFilter)
{
    TNode result = TNode::CreateList();
    for (const auto& attribute : attributeFilter.Attributes_) {
        result.Add(attribute);
    }
    return result;
}

static TNode SerializeAttributeFilter(const TOperationAttributeFilter& attributeFilter)
{
    TNode result = TNode::CreateList();
    for (const auto& attribute : attributeFilter.Attributes_) {
        result.Add(ToString(attribute));
    }
    return result;
}

template <typename TOptions>
static void SetFirstLastTabletIndex(TNode* node, const TOptions& options)
{
    if (options.FirstTabletIndex_) {
        (*node)["first_tablet_index"] = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        (*node)["last_tablet_index"] = *options.LastTabletIndex_;
    }
}

static TString GetDefaultTransactionTitle()
{
    const auto processState = TProcessState::Get();
    TStringStream res;

    res << "User transaction. Created by: " << processState->UserName << " on " << processState->FqdnHostName
        << " client: " << processState->ClientVersion << " pid: " << processState->Pid;
    if (!processState->CommandLine.empty()) {
        res << " program: " << processState->CommandLine[0];
    } else {
        res << " command line is unknown probably NYT::Initialize was never called";
    }

#ifndef NDEBUG
    res << " build: debug";
#endif

    return res.Str();
}

template <typename T>
void SerializeMasterReadOptions(TNode* node, const TMasterReadOptions<T>& options)
{
    if (options.ReadFrom_) {
        (*node)["read_from"] = ToString(*options.ReadFrom_);
    }
}

////////////////////////////////////////////////////////////////////////////////

TNode SerializeParamsForCreate(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    result["recursive"] = options.Recursive_;
    result["type"] = ToString(type);
    result["ignore_existing"] = options.IgnoreExisting_;
    result["force"] = options.Force_;
    if (options.Attributes_) {
        result["attributes"] = *options.Attributes_;
    }
    return result;
}

TNode SerializeParamsForRemove(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TRemoveOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    return result;
}

TNode SerializeParamsForExists(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TExistsOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    SerializeMasterReadOptions(&result, options);
    return result;
}

TNode SerializeParamsForGet(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TGetOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    SerializeMasterReadOptions(&result, options);
    if (options.AttributeFilter_) {
        result["attributes"] = SerializeAttributeFilter(*options.AttributeFilter_);
    }
    if (options.MaxSize_) {
        result["max_size"] = *options.MaxSize_;
    }
    return result;
}

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TSetOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    result["recursive"] = options.Recursive_;
    if (options.Force_) {
        result["force"] = *options.Force_;
    }
    return result;
}

TNode SerializeParamsForMultisetAttributes(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    [[maybe_unused]] const TMultisetAttributesOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    if (options.Force_) {
        result["force"] = *options.Force_;
    }
    return result;
}

TNode SerializeParamsForList(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TListOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    SerializeMasterReadOptions(&result, options);
    if (options.MaxSize_) {
        result["max_size"] = *options.MaxSize_;
    }
    if (options.AttributeFilter_) {
        result["attributes"] = SerializeAttributeFilter(*options.AttributeFilter_);
    }
    return result;
}

TNode SerializeParamsForCopy(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["source_path"] = AddPathPrefix(sourcePath, pathPrefix);
    result["destination_path"] = AddPathPrefix(destinationPath, pathPrefix);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    result["preserve_account"] = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result["preserve_expiration_time"] = *options.PreserveExpirationTime_;
    }
    return result;
}

TNode SerializeParamsForMove(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["source_path"] = AddPathPrefix(sourcePath, pathPrefix);
    result["destination_path"] = AddPathPrefix(destinationPath, pathPrefix);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    result["preserve_account"] = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result["preserve_expiration_time"] = *options.PreserveExpirationTime_;
    }
    return result;
}

TNode SerializeParamsForLink(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["target_path"] = AddPathPrefix(targetPath, pathPrefix);
    result["link_path"] = AddPathPrefix(linkPath, pathPrefix);
    result["recursive"] = options.Recursive_;
    result["ignore_existing"] = options.IgnoreExisting_;
    result["force"] = options.Force_;
    if (options.Attributes_) {
        result["attributes"] = *options.Attributes_;
    }
    return result;
}

TNode SerializeParamsForLock(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    result["mode"] = ToString(mode);
    result["waitable"] = options.Waitable_;
    if (options.AttributeKey_) {
        result["attribute_key"] = *options.AttributeKey_;
    }
    if (options.ChildKey_) {
        result["child_key"] = *options.ChildKey_;
    }
    return result;
}

TNode SerializeParamsForUnlock(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TUnlockOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    Y_UNUSED(options);
    return result;
}

TNode SerializeParamsForConcatenate(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    {
        auto actualDestination = destinationPath;
        actualDestination.Path(AddPathPrefix(actualDestination.Path_, pathPrefix));
        if (options.Append_) {
            actualDestination.Append(*options.Append_);
        }
        result["destination_path"] = PathToNode(actualDestination);
    }
    auto& sourcePathsNode = result["source_paths"];
    for (const auto& path : sourcePaths) {
        auto actualSource = path;
        actualSource.Path(AddPathPrefix(actualSource.Path_, pathPrefix));
        sourcePathsNode.Add(PathToNode(actualSource));
    }
    return result;
}

TNode SerializeParamsForPingTx(
    const TTransactionId& transactionId)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    return result;
}

TNode SerializeParamsForListOperations(
    const TListOperationsOptions& options)
{
    TNode result = TNode::CreateMap();
    if (options.FromTime_) {
        result["from_time"] = ToString(*options.FromTime_);
    }
    if (options.ToTime_) {
        result["to_time"] = ToString(*options.ToTime_);
    }
    if (options.CursorTime_) {
        result["cursor_time"] = ToString(*options.CursorTime_);
    }
    if (options.CursorDirection_) {
        result["cursor_direction"] = ToString(*options.CursorDirection_);
    }
    if (options.Pool_) {
        result["pool"] = *options.Pool_;
    }
    if (options.Filter_) {
        result["filter"] = *options.Filter_;
    }
    if (options.User_) {
        result["user"] = *options.User_;
    }
    if (options.State_) {
        result["state"] = *options.State_;
    }
    if (options.Type_) {
        result["type"] = ToString(*options.Type_);
    }
    if (options.WithFailedJobs_) {
        result["with_failed_jobs"] = *options.WithFailedJobs_;
    }
    if (options.IncludeCounters_) {
        result["include_counters"] = *options.IncludeCounters_;
    }
    if (options.IncludeArchive_) {
        result["include_archive"] = *options.IncludeArchive_;
    }
    if (options.Limit_) {
        result["limit"] = *options.Limit_;
    }
    return result;
}

TNode SerializeParamsForGetOperation(const std::variant<TString, TOperationId>& aliasOrOperationId, const TGetOperationOptions& options)
{
    auto includeRuntime = options.IncludeRuntime_;
    TNode result;
    std::visit([&] (const auto& value) {
        using TValue = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<TValue, TString>) {
            SetAliasParam(&result, value);
            if (includeRuntime.Empty()) {
                // Getting operation by operation alias requires enabling this option.
                // So enable it unless user explicitly set it.
                includeRuntime = true;
            }
        } else if constexpr (std::is_same_v<TValue, TOperationId>) {
            SetOperationIdParam(&result, value);
        } else {
            static_assert(std::is_same_v<TValue, void>, "unreachable");
        }
    }, aliasOrOperationId);
    if (options.AttributeFilter_) {
        result["attributes"] = SerializeAttributeFilter(*options.AttributeFilter_);
    }
    if (includeRuntime.Defined()) {
        result["include_runtime"] = *includeRuntime;
    }
    return result;
}

TNode SerializeParamsForAbortOperation(const TOperationId& operationId)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    return result;
}

TNode SerializeParamsForCompleteOperation(const TOperationId& operationId)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    return result;
}

TNode SerializeParamsForSuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    if (options.AbortRunningJobs_) {
        result["abort_running_jobs"] = *options.AbortRunningJobs_;
    }
    return result;
}

TNode SerializeParamsForResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    Y_UNUSED(options);
    return result;
}

TNode SerializeParamsForUpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    TNode& parameters = result["parameters"];
    if (options.Pool_) {
        parameters["pool"] = *options.Pool_;
    }
    if (options.Weight_) {
        parameters["weight"] = *options.Weight_;
    }
    if (!options.Owners_.empty()) {
        parameters["owners"] = TNode::CreateList();
        for (const auto& owner : options.Owners_) {
            parameters["owners"].Add(owner);
        }
    }
    if (options.SchedulingOptionsPerPoolTree_) {
        parameters["scheduling_options_per_pool_tree"] = TNode::CreateMap();
        for (const auto& entry : options.SchedulingOptionsPerPoolTree_->Options_) {
            auto schedulingOptionsNode = TNode::CreateMap();
            const auto& schedulingOptions = entry.second;
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
            parameters["scheduling_options_per_pool_tree"][entry.first] = std::move(schedulingOptionsNode);
        }
    }
    return result;
}

TNode SerializeParamsForGetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& /* options */)
{
    TNode result;
    SetOperationIdParam(&result, operationId);
    result["job_id"] = GetGuidAsString(jobId);
    return result;
}

TNode SerializeParamsForListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    TNode result;
    SetOperationIdParam(&result, operationId);

    if (options.Type_) {
        result["type"] = ToString(*options.Type_);
    }
    if (options.State_) {
        result["state"] = ToString(*options.State_);
    }
    if (options.Address_) {
        result["address"] = *options.Address_;
    }
    if (options.WithStderr_) {
        result["with_stderr"] = *options.WithStderr_;
    }
    if (options.WithSpec_) {
        result["with_spec"] = *options.WithSpec_;
    }
    if (options.WithFailContext_) {
        result["with_fail_context"] = *options.WithFailContext_;
    }
    if (options.WithMonitoringDescriptor_) {
        result["with_monitoring_descriptor"] = *options.WithMonitoringDescriptor_;
    }

    if (options.SortField_) {
        result["sort_field"] = ToString(*options.SortField_);
    }
    if (options.SortOrder_) {
        result["sort_order"] = ToString(*options.SortOrder_);
    }

    if (options.Offset_) {
        result["offset"] = *options.Offset_;
    }
    if (options.Limit_) {
        result["limit"] = *options.Limit_;
    }

    if (options.IncludeCypress_) {
        result["include_cypress"] = *options.IncludeCypress_;
    }
    if (options.IncludeArchive_) {
        result["include_archive"] = *options.IncludeArchive_;
    }
    if (options.IncludeControllerAgent_) {
        result["include_controller_agent"] = *options.IncludeControllerAgent_;
    }
    return result;
}

TNode SerializeParametersForInsertRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TInsertRowsOptions& options)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    if (options.Aggregate_) {
        result["aggregate"] = *options.Aggregate_;
    }
    if (options.Update_) {
        result["update"] = *options.Update_;
    }
    if (options.Atomicity_) {
        result["atomicity"] = ToString(*options.Atomicity_);
    }
    if (options.Durability_) {
        result["durability"] = ToString(*options.Durability_);
    }
    if (options.RequireSyncReplica_) {
        result["require_sync_replica"] = *options.RequireSyncReplica_;
    }
    return result;
}

TNode SerializeParametersForDeleteRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TDeleteRowsOptions& options)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    if (options.Atomicity_) {
        result["atomicity"] = ToString(*options.Atomicity_);
    }
    if (options.Durability_) {
        result["durability"] = ToString(*options.Durability_);
    }
    if (options.RequireSyncReplica_) {
        result["require_sync_replica"] = *options.RequireSyncReplica_;
    }
    return result;
}

TNode SerializeParametersForTrimRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TTrimRowsOptions& /* options*/)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    return result;
}

TNode SerializeParamsForParseYPath(const TRichYPath& path)
{
    TNode result;
    result["path"] = PathToNode(path);
    return result;
}

TNode SerializeParamsForEnableTableReplica(
    const TReplicaId& replicaId)
{
    TNode result;
    result["replica_id"] = GetGuidAsString(replicaId);
    return result;
}

TNode SerializeParamsForDisableTableReplica(
    const TReplicaId& replicaId)
{
    TNode result;
    result["replica_id"] = GetGuidAsString(replicaId);
    return result;
}

TNode SerializeParamsForAlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options)
{
    TNode result;
    result["replica_id"] = GetGuidAsString(replicaId);
    if (options.Enabled_) {
        result["enabled"] = *options.Enabled_;
    }
    if (options.Mode_) {
        result["mode"] = ToString(*options.Mode_);
    }
    return result;
}

TNode SerializeParamsForFreezeTable(
    const TString& pathPrefix,
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    SetFirstLastTabletIndex(&result, options);
    return result;
}

TNode SerializeParamsForUnfreezeTable(
    const TString& pathPrefix,
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    SetFirstLastTabletIndex(&result, options);
    return result;
}

TNode SerializeParamsForAlterTable(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TAlterTableOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, path);
    if (options.Dynamic_) {
        result["dynamic"] = *options.Dynamic_;
    }
    if (options.Schema_) {
        TNode schema;
        {
            TNodeBuilder builder(&schema);
            Serialize(*options.Schema_, &builder);
        }
        result["schema"] = schema;
    }
    if (options.UpstreamReplicaId_) {
        result["upstream_replica_id"] = GetGuidAsString(*options.UpstreamReplicaId_);
    }
    return result;
}

TNode SerializeParamsForGetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    for (const auto& path : paths) {
        result["paths"].Add(PathToNode(path));
    }
    if (options.FetcherMode_) {
        result["fetcher_mode"] = ToString(*options.FetcherMode_);
    }
    return result;
}

TNode SerializeParamsForGetTablePartitions(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    for (const auto& path : paths) {
        result["paths"].Add(PathToNode(path));
    }
    result["partition_mode"] = ToString(options.PartitionMode_);
    result["data_weight_per_partition"] = options.DataWeightPerPartition_;
    if (options.MaxPartitionCount_) {
        result["max_partition_count"] = *options.MaxPartitionCount_;
    }
    result["adjust_data_weight_per_partition"] = options.AdjustDataWeightPerPartition_;
    return result;
}

TNode SerializeParamsForGetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions&)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["md5"] = md5Signature;
    result["cache_path"] = cachePath;
    return result;
}

TNode SerializeParamsForPutFileToCache(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, pathPrefix, filePath);
    result["md5"] = md5Signature;
    result["cache_path"] = cachePath;
    if (options.PreserveExpirationTimeout_) {
        result["preserve_expiration_timeout"] = *options.PreserveExpirationTimeout_;
    }
    return result;
}

TNode SerializeParamsForSkyShareTable(
    const TString& serverName,
    const TString& pathPrefix,
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options)
{
    TNode result;

    if (tablePaths.size() == 1) {
        SetPathParam(&result, pathPrefix, tablePaths[0]);
    } else {
        auto pathList = TNode::CreateList();
        for (const auto& p : tablePaths) {
            pathList.Add(AddPathPrefix(p, pathPrefix));
        }
        result["paths"] = pathList;
    }
    result["cluster"] = serverName;

    if (options.KeyColumns_) {
        auto keyColumnsList = TNode::CreateList();
        for (const auto& s : options.KeyColumns_->Parts_) {
            if (s.empty()) {
                continue;
            }
            keyColumnsList.Add(s);
        }
        result["key_columns"] = keyColumnsList;
    }

    if (options.EnableFastbone_) {
        result["enable_fastbone"] = *options.EnableFastbone_;
    }

    return result;
}

TNode SerializeParamsForCheckPermission(
    const TString& user,
    EPermission permission,
    const TString& pathPrefix,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    result["path"] = path;
    result["user"] = user;
    result["permission"] = ToString(permission);
    if (!options.Columns_.empty()) {
        result["columns"] = TNode::CreateList();
        result["columns"].AsList().assign(options.Columns_.begin(), options.Columns_.end());
    }
    return result;
}

TNode SerializeParamsForGetTabletInfos(
    const TString& pathPrefix,
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    Y_UNUSED(options);
    TNode result;
    SetPathParam(&result, pathPrefix, path);
    result["tablet_indexes"] = TNode::CreateList();
    result["tablet_indexes"].AsList().assign(tabletIndexes.begin(), tabletIndexes.end());
    return result;
}

TNode SerializeParamsForAbortTransaction(const TTransactionId& transactionId)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    return result;
}

TNode SerializeParamsForCommitTransaction(const TTransactionId& transactionId)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    return result;
}

TNode SerializeParamsForStartTransaction(
    const TTransactionId& parentTransactionId,
    TDuration txTimeout,
    const TStartTransactionOptions& options)
{
    TNode result;

    SetTransactionIdParam(&result, parentTransactionId);
    result["timeout"] = static_cast<i64>((options.Timeout_.GetOrElse(txTimeout).MilliSeconds()));
    if (options.Deadline_) {
        result["deadline"] = ToString(options.Deadline_);
    }

    if (options.PingAncestors_) {
        result["ping_ancestor_transactions"] = true;
    }

    if (options.Attributes_ && !options.Attributes_->IsMap()) {
        ythrow TApiUsageError() << "Attributes must be a Map node";
    }

    auto attributes = options.Attributes_.GetOrElse(TNode::CreateMap());
    if (options.Title_) {
        attributes["title"] = *options.Title_;
    } else if (!attributes.HasKey("title")) {
        attributes["title"] = GetDefaultTransactionTitle();
    }
    result["attributes"] = attributes;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
