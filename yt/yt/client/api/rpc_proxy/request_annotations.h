#pragma once

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/file_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void AnnotateReadTableRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req);

void AnnotateReadFileRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NProto::TReqReadFile& req);

void AnnotateWriteTableRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path);

void AnnotateWriteFileRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req);

void AnnotatePartitionTablesRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req);

void AnnotateReadTablePartitionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NProto::TReqReadTablePartition& req);

void AnnotateStartDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path);

void AnnotatePingDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId);

void AnnotatePingDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedDistributedWriteSessionPtr& session);

void AnnotateFinishDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId);

void AnnotateFinishDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedDistributedWriteSessionPtr& session);

void AnnotateWriteTableFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId);

void AnnotateWriteTableFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedWriteFragmentCookiePtr& cookie);

void AnnotateStartDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path);

void AnnotatePingDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId);

void AnnotatePingDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session);

void AnnotateFinishDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId);

void AnnotateFinishDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session);

void AnnotateWriteFileFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId);

void AnnotateWriteFileFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedWriteFileFragmentCookiePtr& cookie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
