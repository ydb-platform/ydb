#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const NApi::TTimeoutOptions& options);

[[noreturn]] void ThrowUnimplemented(const TString& method);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NProto::TTransactionalOptions* proto,
    const NApi::TTransactionalOptions& options);

void ToProto(
    NProto::TPrerequisiteOptions* proto,
    const NApi::TPrerequisiteOptions& options);

void ToProto(
    NProto::TMasterReadOptions* proto,
    const NApi::TMasterReadOptions& options);

void ToProto(
    NProto::TMutatingOptions* proto,
    const NApi::TMutatingOptions& options);

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const NApi::TSuppressableAccessTrackingOptions& options);

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const NApi::TTabletRangeOptions& options);

void ToProto(
    NProto::TRetentionConfig* protoConfig,
    const NTableClient::TRetentionConfig& config);

void FromProto(
    NTableClient::TRetentionConfig* config,
    const NProto::TRetentionConfig& protoConfig);

void ToProto(
    NProto::TGetFileFromCacheResult* proto,
    const NApi::TGetFileFromCacheResult& result);

void FromProto(
    NApi::TGetFileFromCacheResult* result,
    const NProto::TGetFileFromCacheResult& proto);

void ToProto(
    NProto::TPutFileToCacheResult* proto,
    const NApi::TPutFileToCacheResult& result);

void FromProto(
    NApi::TPutFileToCacheResult* result,
    const NProto::TPutFileToCacheResult& proto);

void ToProto(
    NProto::TCheckPermissionResult* proto,
    const NApi::TCheckPermissionResult& result);

void FromProto(
    NApi::TCheckPermissionResult* result,
    const NProto::TCheckPermissionResult& proto);

void ToProto(
    NProto::TCheckPermissionByAclResult* proto,
    const NApi::TCheckPermissionByAclResult& result);

void FromProto(
    NApi::TCheckPermissionByAclResult* result,
    const NProto::TCheckPermissionByAclResult& proto);

void ToProto(
    NProto::TListOperationsResult* proto,
    const NApi::TListOperationsResult& result);

void FromProto(
    NApi::TListOperationsResult* result,
    const NProto::TListOperationsResult& proto);

void ToProto(
    NProto::TListJobsResult* proto,
    const NApi::TListJobsResult& result);

void FromProto(
    NApi::TListJobsResult* result,
    const NProto::TListJobsResult& proto);

void ToProto(NProto::TColumnSchema* protoSchema, const NTableClient::TColumnSchema& schema);
void FromProto(NTableClient::TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchema& schema);
void FromProto(NTableClient::TTableSchema* schema, const NProto::TTableSchema& protoSchema);

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchemaPtr& schema);
void FromProto(NTableClient::TTableSchemaPtr* schema, const NProto::TTableSchema& protoSchema);

// Doesn't fill cell_config_version.
void ToProto(
    NProto::TTabletInfo* protoTabletInfo,
    const NTabletClient::TTabletInfo& tabletInfo);
// Doesn't fill TableId, UpdateTime and Owners.
void FromProto(
    NTabletClient::TTabletInfo* tabletInfo,
    const NProto::TTabletInfo& protoTabletInfo);

void ToProto(
    NProto::TTabletReadOptions* protoOptions,
    const NApi::TTabletReadOptionsBase& options);

void ToProto(
    NProto::TQueryStatistics* protoStatistics,
    const NQueryClient::TQueryStatistics& statistics);

void FromProto(
    NQueryClient::TQueryStatistics* statistics,
    const NProto::TQueryStatistics& protoStatistics);

void ToProto(
    NProto::TOperation* protoOperation,
    const NApi::TOperation& operation);

void FromProto(
    NApi::TOperation* operation,
    const NProto::TOperation& protoOperation);

void ToProto(
    NProto::TJob* protoJob,
    const NApi::TJob& job);

void FromProto(
    NApi::TJob* job,
    const NProto::TJob& protoJob);

void ToProto(
    NProto::TListJobsStatistics* protoStatistics,
    const NApi::TListJobsStatistics& statistics);

void FromProto(
    NApi::TListJobsStatistics* statistics,
    const NProto::TListJobsStatistics& protoStatistics);

void ToProto(
    NProto::TFetchChunkSpecConfig* protoFetchChunkSpecConfig,
    const NChunkClient::TFetchChunkSpecConfigPtr& fetchChunkSpecConfig);

void FromProto(
    const NChunkClient::TFetchChunkSpecConfigPtr& fetchChunkSpecConfig,
    const NProto::TFetchChunkSpecConfig& protoFetchChunkSpecConfig);

void ToProto(
    NProto::TFetcherConfig* protoFetcherConfig,
    const NChunkClient::TFetcherConfigPtr& fetcherConfig);

void FromProto(
    const NChunkClient::TFetcherConfigPtr& fetcherConfig,
    const NProto::TFetcherConfig& protoFetcherConfig);

void ToProto(
    NProto::TColumnarStatistics* protoStatistics,
    const NTableClient::TColumnarStatistics& statistics);

void FromProto(
    NTableClient::TColumnarStatistics* statistics,
    const NProto::TColumnarStatistics& protoStatistics);

void ToProto(
    NProto::TMultiTablePartition* protoMultiTablePartition,
    const NApi::TMultiTablePartition& multiTablePartition);

void FromProto(
    NApi::TMultiTablePartition* multiTablePartition,
    const NProto::TMultiTablePartition& protoMultiTablePartition);

void FromProto(
    NApi::TMultiTablePartitions* multiTablePartitions,
    const NProto::TRspPartitionTables& protoRspPartitionTables);

void ToProto(
    NProto::TRowBatchReadOptions* proto,
    const NQueueClient::TQueueRowBatchReadOptions& result);

void FromProto(
    NQueueClient::TQueueRowBatchReadOptions* result,
    const NProto::TRowBatchReadOptions& proto);

void ToProto(
    NProto::TTableBackupManifest* protoManifest,
    const NApi::TTableBackupManifestPtr& manifest);

void FromProto(
    NApi::TTableBackupManifestPtr* manifest,
    const NProto::TTableBackupManifest& protoManifest);

void ToProto(
    NProto::TBackupManifest::TClusterManifest* protoEntry,
    const std::pair<TString, std::vector<NApi::TTableBackupManifestPtr>>& entry);

void FromProto(
    std::pair<TString, std::vector<NApi::TTableBackupManifestPtr>>* entry,
    const NProto::TBackupManifest::TClusterManifest& protoEntry);

void ToProto(
    NProto::TBackupManifest* protoManifest,
    const NApi::TBackupManifest& manifest);

void FromProto(
    NApi::TBackupManifest* manifest,
    const NProto::TBackupManifest& protoManifest);

void ToProto(
    NProto::TQuery* protoQuery,
    const NApi::TQuery& query);

void FromProto(
    NApi::TQuery* query,
    const NProto::TQuery& protoQuery);

NProto::EOperationType ConvertOperationTypeToProto(
    NScheduler::EOperationType operationType);

NScheduler::EOperationType ConvertOperationTypeFromProto(
    NProto::EOperationType proto);

NProto::EOperationState ConvertOperationStateToProto(
    NScheduler::EOperationState operationState);

NScheduler::EOperationState ConvertOperationStateFromProto(
    NProto::EOperationState proto);

NProto::EJobType ConvertJobTypeToProto(
    NJobTrackerClient::EJobType jobType);

NJobTrackerClient::EJobType ConvertJobTypeFromProto(
    NProto::EJobType proto);

NProto::EJobState ConvertJobStateToProto(
    NJobTrackerClient::EJobState jobState);

NJobTrackerClient::EJobState ConvertJobStateFromProto(
    NProto::EJobState proto);

NProto::EQueryEngine ConvertQueryEngineToProto(
    NQueryTrackerClient::EQueryEngine queryEngine);

NQueryTrackerClient::EQueryEngine ConvertQueryEngineFromProto(
    NProto::EQueryEngine proto);

NProto::EQueryState ConvertQueryStateToProto(
    NQueryTrackerClient::EQueryState queryState);

NQueryTrackerClient::EQueryState ConvertQueryStateFromProto(
    NProto::EQueryState proto);
} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error, bool retryProxyBanned = true);

////////////////////////////////////////////////////////////////////////////////

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind,
    NProto::ERowsetFormat expectedFormat);

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TTableSchema& schema,
    TRange<TRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data,
    NTableClient::TRowBufferPtr buffer = nullptr);

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TTableSchema& schema,
    TRange<NTableClient::TTypeErasedRow> rows,
    NProto::TRowsetDescriptor* descriptor,
    bool versioned);

TIntrusivePtr<NApi::IRowset<NTableClient::TTypeErasedRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data,
    bool versioned);

////////////////////////////////////////////////////////////////////////////////

//! Invokes std::stable_sort reordering addresses by the index of the first regex they match;
//! addresses not matching any regex are placed at the very end.
void SortByRegexes(std::vector<TString>& values, const std::vector<NRe2::TRe2Ptr>& regexes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
