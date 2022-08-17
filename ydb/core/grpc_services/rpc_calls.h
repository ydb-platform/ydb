#pragma once
#include "defs.h"

#include "local_rate_limiter.h"

#include <ydb/core/grpc_services/base/base.h>

#include <ydb/public/api/protos/ydb_auth.pb.h>
#include <ydb/public/api/protos/ydb_clickhouse_internal.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/protos/ydb_experimental.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_s3_internal.pb.h>
#include <ydb/public/api/protos/ydb_persqueue_cluster_discovery.pb.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/public/api/protos/yq.pb.h>

#include <ydb/public/api/grpc/draft/dummy.pb.h>
#include <ydb/public/api/grpc/draft/ydb_long_tx_v1.pb.h>

#include <ydb/public/lib/operation_id/operation_id.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NGRpcService {

template <>
void FillYdbStatus(Ydb::PersQueue::V1::StreamingWriteServerMessage& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

template <>
void FillYdbStatus(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

template <>
void FillYdbStatus(Ydb::Topic::StreamWriteMessage::FromServer& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

template <>
void FillYdbStatus(Ydb::Topic::StreamReadMessage::FromServer& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

template <>
void FillYdbStatus(Draft::Dummy::PingResponse& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

template <>
void FillYdbStatus(Ydb::Coordination::SessionResponse& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

using TEvAlterTableRequest = TGRpcRequestValidationWrapper<TRpcServices::EvAlterTable, Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse, true, TRateLimiterMode::Rps>;
using TEvCreateTableRequest = TGRpcRequestValidationWrapper<TRpcServices::EvCreateTable, Ydb::Table::CreateTableRequest, Ydb::Table::CreateTableResponse, true, TRateLimiterMode::Rps>;
using TEvDropTableRequest = TGRpcRequestWrapper<TRpcServices::EvDropTable, Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse, true, TRateLimiterMode::Rps>;
using TEvCopyTableRequest = TGRpcRequestWrapper<TRpcServices::EvCopyTable, Ydb::Table::CopyTableRequest, Ydb::Table::CopyTableResponse, true, TRateLimiterMode::Rps>;
using TEvCopyTablesRequest = TGRpcRequestWrapper<TRpcServices::EvCopyTables, Ydb::Table::CopyTablesRequest, Ydb::Table::CopyTablesResponse, true, TRateLimiterMode::Rps>;
using TEvRenameTablesRequest = TGRpcRequestWrapper<TRpcServices::EvRenameTables, Ydb::Table::RenameTablesRequest, Ydb::Table::RenameTablesResponse, true, TRateLimiterMode::Rps>;
using TEvDescribeTableRequest = TGRpcRequestWrapper<TRpcServices::EvDescribeTable, Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse, true, TRateLimiterMode::Rps>;
using TEvGetOperationRequest = TGRpcRequestValidationWrapper<TRpcServices::EvGetOperation, Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse, true, TRateLimiterMode::Rps>;
using TEvCancelOperationRequest = TGRpcRequestValidationWrapper<TRpcServices::EvCancelOperation, Ydb::Operations::CancelOperationRequest, Ydb::Operations::CancelOperationResponse, false, TRateLimiterMode::Rps>;
using TEvForgetOperationRequest = TGRpcRequestValidationWrapper<TRpcServices::EvForgetOperation, Ydb::Operations::ForgetOperationRequest, Ydb::Operations::ForgetOperationResponse, false, TRateLimiterMode::Rps>;
using TEvListOperationsRequest = TGRpcRequestValidationWrapper<TRpcServices::EvListOperations, Ydb::Operations::ListOperationsRequest, Ydb::Operations::ListOperationsResponse, false, TRateLimiterMode::Rps>;
using TEvCreateSessionRequest = TGRpcRequestWrapper<TRpcServices::EvCreateSession, Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse, true, TRateLimiterMode::Rps>;
using TEvDeleteSessionRequest = TGRpcRequestWrapper<TRpcServices::EvDeleteSession, Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse, true>;
using TEvKeepAliveRequest = TGRpcRequestWrapper<TRpcServices::EvKeepAlive, Ydb::Table::KeepAliveRequest, Ydb::Table::KeepAliveResponse, true, TRateLimiterMode::Rps>;
using TEvReadTableRequest = TGRpcRequestWrapper<TRpcServices::EvReadTable, Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse, false, TRateLimiterMode::RuOnProgress>;
using TEvExplainDataQueryRequest = TGRpcRequestWrapper<TRpcServices::EvExplainDataQuery, Ydb::Table::ExplainDataQueryRequest, Ydb::Table::ExplainDataQueryResponse, true, TRateLimiterMode::Rps>;
using TEvPrepareDataQueryRequest = TGRpcRequestWrapper<TRpcServices::EvPrepareDataQuery, Ydb::Table::PrepareDataQueryRequest, Ydb::Table::PrepareDataQueryResponse, true, TRateLimiterMode::Ru>;
using TEvExecuteDataQueryRequest = TGRpcRequestWrapper<TRpcServices::EvExecuteDataQuery, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse, true, TRateLimiterMode::Ru>;
using TEvExecuteSchemeQueryRequest = TGRpcRequestWrapper<TRpcServices::EvExecuteSchemeQuery, Ydb::Table::ExecuteSchemeQueryRequest, Ydb::Table::ExecuteSchemeQueryResponse, true, TRateLimiterMode::Rps>;
using TEvBeginTransactionRequest = TGRpcRequestWrapper<TRpcServices::EvBeginTransaction, Ydb::Table::BeginTransactionRequest, Ydb::Table::BeginTransactionResponse, true, TRateLimiterMode::Rps>;
using TEvCommitTransactionRequest = TGRpcRequestWrapper<TRpcServices::EvCommitTransaction, Ydb::Table::CommitTransactionRequest, Ydb::Table::CommitTransactionResponse, true>;
using TEvRollbackTransactionRequest = TGRpcRequestWrapper<TRpcServices::EvRollbackTransaction, Ydb::Table::RollbackTransactionRequest, Ydb::Table::RollbackTransactionResponse, true>;
using TEvCreateTenantRequest = TGRpcRequestWrapper<TRpcServices::EvCreateTenant, Ydb::Cms::CreateDatabaseRequest, Ydb::Cms::CreateDatabaseResponse, true>;
using TEvAlterTenantRequest = TGRpcRequestWrapper<TRpcServices::EvAlterTenant, Ydb::Cms::AlterDatabaseRequest, Ydb::Cms::AlterDatabaseResponse, true>;
using TEvGetTenantStatusRequest = TGRpcRequestWrapper<TRpcServices::EvGetTenantStatus, Ydb::Cms::GetDatabaseStatusRequest, Ydb::Cms::GetDatabaseStatusResponse, true>;
using TEvListTenantsRequest = TGRpcRequestWrapper<TRpcServices::EvListTenants, Ydb::Cms::ListDatabasesRequest, Ydb::Cms::ListDatabasesResponse, true>;
using TEvRemoveTenantRequest = TGRpcRequestWrapper<TRpcServices::EvRemoveTenant, Ydb::Cms::RemoveDatabaseRequest, Ydb::Cms::RemoveDatabaseResponse, true>;
using TEvListEndpointsRequest = TGRpcRequestWrapper<TRpcServices::EvListEndpoints, Ydb::Discovery::ListEndpointsRequest, Ydb::Discovery::ListEndpointsResponse, true>;
using TEvDescribeTenantOptionsRequest = TGRpcRequestWrapper<TRpcServices::EvDescribeTenantOptions, Ydb::Cms::DescribeDatabaseOptionsRequest, Ydb::Cms::DescribeDatabaseOptionsResponse, true>;
using TEvDescribeTableOptionsRequest = TGRpcRequestWrapper<TRpcServices::EvDescribeTableOptions, Ydb::Table::DescribeTableOptionsRequest, Ydb::Table::DescribeTableOptionsResponse, true, TRateLimiterMode::Rps>;
using TEvCreateCoordinationNode = TGRpcRequestWrapper<TRpcServices::EvCreateCoordinationNode, Ydb::Coordination::CreateNodeRequest, Ydb::Coordination::CreateNodeResponse, true, TRateLimiterMode::Rps>;
using TEvAlterCoordinationNode = TGRpcRequestWrapper<TRpcServices::EvAlterCoordinationNode, Ydb::Coordination::AlterNodeRequest, Ydb::Coordination::AlterNodeResponse, true, TRateLimiterMode::Rps>;
using TEvDropCoordinationNode = TGRpcRequestWrapper<TRpcServices::EvDropCoordinationNode, Ydb::Coordination::DropNodeRequest, Ydb::Coordination::DropNodeResponse, true, TRateLimiterMode::Rps>;
using TEvDescribeCoordinationNode = TGRpcRequestWrapper<TRpcServices::EvDescribeCoordinationNode, Ydb::Coordination::DescribeNodeRequest, Ydb::Coordination::DescribeNodeResponse, true, TRateLimiterMode::Rps>;
using TEvReadColumnsRequest = TGRpcRequestWrapper<TRpcServices::EvReadColumns, Ydb::ClickhouseInternal::ScanRequest, Ydb::ClickhouseInternal::ScanResponse, true>;
using TEvGetShardLocationsRequest = TGRpcRequestWrapper<TRpcServices::EvGetShardLocations, Ydb::ClickhouseInternal::GetShardLocationsRequest, Ydb::ClickhouseInternal::GetShardLocationsResponse, true>;
using TEvKikhouseDescribeTableRequest = TGRpcRequestWrapper<TRpcServices::EvKikhouseDescribeTable, Ydb::ClickhouseInternal::DescribeTableRequest, Ydb::ClickhouseInternal::DescribeTableResponse, true>;
using TEvS3ListingRequest = TGRpcRequestWrapper<TRpcServices::EvS3Listing, Ydb::S3Internal::S3ListingRequest, Ydb::S3Internal::S3ListingResponse, true>;
using TEvBiStreamPingRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvBiStreamPing, Draft::Dummy::PingRequest, Draft::Dummy::PingResponse>;
using TEvExperimentalStreamQueryRequest = TGRpcRequestWrapper<TRpcServices::EvExperimentalStreamQuery, Ydb::Experimental::ExecuteStreamQueryRequest, Ydb::Experimental::ExecuteStreamQueryResponse, false>;
using TEvStreamPQWriteRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvStreamPQWrite, Ydb::PersQueue::V1::StreamingWriteClientMessage, Ydb::PersQueue::V1::StreamingWriteServerMessage>;
using TEvStreamPQMigrationReadRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvStreamPQMigrationRead, Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>;
using TEvStreamTopicWriteRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvStreamTopicWrite, Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer>;
using TEvStreamTopicReadRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvStreamTopicRead, Ydb::Topic::StreamReadMessage::FromClient, Ydb::Topic::StreamReadMessage::FromServer>;
using TEvPQReadInfoRequest = TGRpcRequestWrapper<TRpcServices::EvPQReadInfo, Ydb::PersQueue::V1::ReadInfoRequest, Ydb::PersQueue::V1::ReadInfoResponse, true>;
using TEvPQDropTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQDropTopic, Ydb::PersQueue::V1::DropTopicRequest, Ydb::PersQueue::V1::DropTopicResponse, true>;
using TEvPQCreateTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQCreateTopic, Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse, true>;
using TEvPQAlterTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQAlterTopic, Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse, true>;
using TEvPQDescribeTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQDescribeTopic, Ydb::PersQueue::V1::DescribeTopicRequest, Ydb::PersQueue::V1::DescribeTopicResponse, true>;
using TEvPQAddReadRuleRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQAddReadRule, Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse, true>;
using TEvPQRemoveReadRuleRequest = TGRpcRequestValidationWrapper<TRpcServices::EvPQRemoveReadRule, Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse, true>;
using TEvExportToYtRequest = TGRpcRequestValidationWrapper<TRpcServices::EvExportToYt, Ydb::Export::ExportToYtRequest, Ydb::Export::ExportToYtResponse, true>;
using TEvExportToS3Request = TGRpcRequestValidationWrapper<TRpcServices::EvExportToS3, Ydb::Export::ExportToS3Request, Ydb::Export::ExportToS3Response, true>;
using TEvImportFromS3Request = TGRpcRequestValidationWrapper<TRpcServices::EvImportFromS3, Ydb::Import::ImportFromS3Request, Ydb::Import::ImportFromS3Response, true>;
using TEvImportDataRequest = TGRpcRequestValidationWrapper<TRpcServices::EvImportData, Ydb::Import::ImportDataRequest, Ydb::Import::ImportDataResponse, true>;
using TEvDropTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvDropTopic, Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse, true>;
using TEvCreateTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvCreateTopic, Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse, true>;
using TEvAlterTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvAlterTopic, Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse, true>;
using TEvDescribeTopicRequest = TGRpcRequestValidationWrapper<TRpcServices::EvDescribeTopic, Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse, true>;

using TEvDiscoverPQClustersRequest = TGRpcRequestWrapper<TRpcServices::EvDiscoverPQClusters, Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest, Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse, true>;
using TEvBulkUpsertRequest = TGRpcRequestWrapper<TRpcServices::EvBulkUpsert, Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse, true, TRateLimiterMode::Ru>;
using TEvWhoAmIRequest = TGRpcRequestWrapper<TRpcServices::EvWhoAmI, Ydb::Discovery::WhoAmIRequest, Ydb::Discovery::WhoAmIResponse, true, TRateLimiterMode::Rps>;
using TEvCreateRateLimiterResource = TGRpcRequestWrapper<TRpcServices::EvCreateRateLimiterResource, Ydb::RateLimiter::CreateResourceRequest, Ydb::RateLimiter::CreateResourceResponse, true, TRateLimiterMode::Rps>;
using TEvAlterRateLimiterResource = TGRpcRequestWrapper<TRpcServices::EvAlterRateLimiterResource, Ydb::RateLimiter::AlterResourceRequest, Ydb::RateLimiter::AlterResourceResponse, true, TRateLimiterMode::Rps>;
using TEvDropRateLimiterResource = TGRpcRequestWrapper<TRpcServices::EvDropRateLimiterResource, Ydb::RateLimiter::DropResourceRequest, Ydb::RateLimiter::DropResourceResponse, true, TRateLimiterMode::Rps>;
using TEvListRateLimiterResources = TGRpcRequestWrapper<TRpcServices::EvListRateLimiterResources, Ydb::RateLimiter::ListResourcesRequest, Ydb::RateLimiter::ListResourcesResponse, true, TRateLimiterMode::Rps>;
using TEvDescribeRateLimiterResource = TGRpcRequestWrapper<TRpcServices::EvDescribeRateLimiterResource, Ydb::RateLimiter::DescribeResourceRequest, Ydb::RateLimiter::DescribeResourceResponse, true, TRateLimiterMode::Rps>;
using TEvAcquireRateLimiterResource = TGRpcRequestWrapper<TRpcServices::EvAcquireRateLimiterResource, Ydb::RateLimiter::AcquireResourceRequest, Ydb::RateLimiter::AcquireResourceResponse, true>;
using TEvKikhouseCreateSnapshotRequest = TGRpcRequestWrapper<TRpcServices::EvKikhouseCreateSnapshot, Ydb::ClickhouseInternal::CreateSnapshotRequest, Ydb::ClickhouseInternal::CreateSnapshotResponse, true>;
using TEvKikhouseRefreshSnapshotRequest = TGRpcRequestWrapper<TRpcServices::EvKikhouseRefreshSnapshot, Ydb::ClickhouseInternal::RefreshSnapshotRequest, Ydb::ClickhouseInternal::RefreshSnapshotResponse, true>;
using TEvKikhouseDiscardSnapshotRequest = TGRpcRequestWrapper<TRpcServices::EvKikhouseDiscardSnapshot, Ydb::ClickhouseInternal::DiscardSnapshotRequest, Ydb::ClickhouseInternal::DiscardSnapshotResponse, true>;
using TEvSelfCheckRequest = TGRpcRequestWrapper<TRpcServices::EvSelfCheck, Ydb::Monitoring::SelfCheckRequest, Ydb::Monitoring::SelfCheckResponse, true>;
using TEvLoginRequest = TGRpcRequestWrapperNoAuth<TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;
using TEvStreamExecuteScanQueryRequest = TGRpcRequestWrapper<TRpcServices::EvStreamExecuteScanQuery, Ydb::Table::ExecuteScanQueryRequest, Ydb::Table::ExecuteScanQueryPartialResponse, false, TRateLimiterMode::RuOnProgress>;
using TEvCoordinationSessionRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvCoordinationSession, Ydb::Coordination::SessionRequest, Ydb::Coordination::SessionResponse>;
using TEvLongTxBeginRequest = TGRpcRequestWrapper<TRpcServices::EvLongTxBegin, Ydb::LongTx::BeginTransactionRequest, Ydb::LongTx::BeginTransactionResponse, true>;
using TEvLongTxCommitRequest = TGRpcRequestWrapper<TRpcServices::EvLongTxCommit, Ydb::LongTx::CommitTransactionRequest, Ydb::LongTx::CommitTransactionResponse, true>;
using TEvLongTxRollbackRequest = TGRpcRequestWrapper<TRpcServices::EvLongTxRollback, Ydb::LongTx::RollbackTransactionRequest, Ydb::LongTx::RollbackTransactionResponse, true>;
using TEvLongTxWriteRequest = TGRpcRequestWrapper<TRpcServices::EvLongTxWrite, Ydb::LongTx::WriteRequest, Ydb::LongTx::WriteResponse, true>;
using TEvLongTxReadRequest = TGRpcRequestWrapper<TRpcServices::EvLongTxRead, Ydb::LongTx::ReadRequest, Ydb::LongTx::ReadResponse, true>;

} // namespace NGRpcService
} // namespace NKikimr
