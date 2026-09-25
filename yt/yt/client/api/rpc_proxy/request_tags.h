#pragma once

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/file_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <library/cpp/yt/logging/tag.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Tags describing an API request, spliced via |Annotate().With(tags)| by both the client
//! and the proxy so that the two ends annotate a call identically.
/*!
 *  The overloads taking a signed payload yield an empty list when it fails to parse;
 *  the server validates the payload anyway.
 */

NLogging::TLoggingTagList MakeReadTableRequestTags(
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req);

NLogging::TLoggingTagList MakeReadFileRequestTags(
    const NProto::TReqReadFile& req);

NLogging::TLoggingTagList MakeWriteTableRequestTags(
    const NYPath::TRichYPath& path);

NLogging::TLoggingTagList MakeWriteFileRequestTags(
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req);

NLogging::TLoggingTagList MakePartitionTablesRequestTags(
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req);

NLogging::TLoggingTagList MakeReadTablePartitionRequestTags(
    const NProto::TReqReadTablePartition& req);

NLogging::TLoggingTagList MakePartitionFileRequestTags(
    const NProto::TReqPartitionFile& req);

NLogging::TLoggingTagList MakeReadFilePartitionRequestTags(
    const NProto::TReqReadFilePartition& req);

NLogging::TLoggingTagList MakeStartDistributedWriteSessionRequestTags(
    const NYPath::TRichYPath& path);

NLogging::TLoggingTagList MakePingDistributedWriteSessionRequestTags(
    NObjectClient::TObjectId tableId);

NLogging::TLoggingTagList MakePingDistributedWriteSessionRequestTags(
    const NTableClient::TSignedDistributedWriteSessionPtr& session);

NLogging::TLoggingTagList MakeFinishDistributedWriteSessionRequestTags(
    NObjectClient::TObjectId tableId);

NLogging::TLoggingTagList MakeFinishDistributedWriteSessionRequestTags(
    const NTableClient::TSignedDistributedWriteSessionPtr& session);

NLogging::TLoggingTagList MakeWriteTableFragmentRequestTags(
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId);

NLogging::TLoggingTagList MakeWriteTableFragmentRequestTags(
    const NTableClient::TSignedWriteFragmentCookiePtr& cookie);

NLogging::TLoggingTagList MakeStartDistributedWriteFileSessionRequestTags(
    const NYPath::TRichYPath& path);

NLogging::TLoggingTagList MakePingDistributedWriteFileSessionRequestTags(
    NObjectClient::TObjectId fileId);

NLogging::TLoggingTagList MakePingDistributedWriteFileSessionRequestTags(
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session);

NLogging::TLoggingTagList MakeFinishDistributedWriteFileSessionRequestTags(
    NObjectClient::TObjectId fileId);

NLogging::TLoggingTagList MakeFinishDistributedWriteFileSessionRequestTags(
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session);

NLogging::TLoggingTagList MakeWriteFileFragmentRequestTags(
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId);

NLogging::TLoggingTagList MakeWriteFileFragmentRequestTags(
    const NFileClient::TSignedWriteFileFragmentCookiePtr& cookie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
