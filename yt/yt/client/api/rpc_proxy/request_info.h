#pragma once

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/file_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TPtr>
void SetReadTableRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req);

template <class TPtr>
void SetReadFileRequestInfo(
    const TPtr& target,
    const NProto::TReqReadFile& req);

template <class TPtr>
void SetWriteTableRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path);

template <class TPtr>
void SetWriteFileRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req);

template <class TPtr>
void SetPartitionTablesRequestInfo(
    const TPtr& target,
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req);

template <class TPtr>
void SetReadTablePartitionRequestInfo(
    const TPtr& target,
    const NProto::TReqReadTablePartition& req);

template <class TPtr>
void SetStartDistributedWriteSessionRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path);

template <class TPtr>
void SetPingDistributedWriteSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId);

template <class TPtr>
void SetFinishDistributedWriteSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId);

template <class TPtr>
void SetWriteTableFragmentRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId);

template <class TPtr>
void SetStartDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path);

template <class TPtr>
void SetPingDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId);

template <class TPtr>
void SetFinishDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId);

template <class TPtr>
void SetWriteFileFragmentRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

#define REQUEST_INFO_INL_H_
#include "request_info-inl.h"
#undef REQUEST_INFO_INL_H_
