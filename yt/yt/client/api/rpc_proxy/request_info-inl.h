#ifndef REQUEST_INFO_INL_H_
#error "Direct inclusion of this file is not allowed, include request_info.h"
// For the sake of sane code completion.
#include "request_info.h"
#endif

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/distributed_table_session.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TPayload>
std::optional<TPayload> TryParseSignedPayload(const NSignature::TSignaturePtr& signature)
{
    if (!signature) {
        return std::nullopt;
    }

    try {
        return NYTree::ConvertTo<TPayload>(NYson::TYsonStringBuf(signature->Payload()));
    } catch (const std::exception&) {
        // Do not fail here, let server validate the payload.
        return std::nullopt;
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TPtr>
void SetReadTableRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req)
{
    target->SetRequestInfo(
        "Path: %v, Unordered: %v, OmitInaccessibleColumns: %v, OmitInaccessibleRows: %v, "
        "DesiredRowsetFormat: %v, ArrowFallbackRowsetFormat: %v",
        path,
        req.unordered(),
        req.omit_inaccessible_columns(),
        req.omit_inaccessible_rows(),
        NProto::ERowsetFormat_Name(req.desired_rowset_format()),
        NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

template <class TPtr>
void SetReadFileRequestInfo(
    const TPtr& target,
    const NProto::TReqReadFile& req)
{
    target->SetRequestInfo(
        "Path: %v, Offset: %v, Length: %v",
        req.path(),
        YT_OPTIONAL_FROM_PROTO(req, offset),
        YT_OPTIONAL_FROM_PROTO(req, length));
}

template <class TPtr>
void SetWriteTableRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path)
{
    target->SetRequestInfo(
        "Path: %v",
        path);
}

template <class TPtr>
void SetWriteFileRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req)
{
    target->SetRequestInfo(
        "Path: %v, ComputeMD5: %v",
        path,
        req.compute_md5());
}

template <class TPtr>
void SetPartitionTablesRequestInfo(
    const TPtr& target,
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req)
{
    target->SetRequestInfo(
        "Paths: %v, PartitionMode: %v, KeyGuarantee: %v, DataWeightPerPartition: %v, "
        "CompressedDataSizePerPartition: %v, MaxPartitionCount: %v, AdjustDataWeightPerPartition: %v, "
        "EnableCookies: %v, FetchCookieNodeDescriptors: %v, OmitInaccessibleRows: %v",
        paths,
        FromProto<NTableClient::ETablePartitionMode>(req.partition_mode()),
        req.enable_key_guarantee(),
        YT_OPTIONAL_FROM_PROTO(req, data_weight_per_partition),
        YT_OPTIONAL_FROM_PROTO(req, compressed_data_size_per_partition),
        YT_OPTIONAL_FROM_PROTO(req, max_partition_count),
        req.adjust_data_weight_per_partition(),
        req.enable_cookies(),
        req.fetch_cookie_node_descriptors(),
        req.omit_inaccessible_rows());
}

template <class TPtr>
void SetReadTablePartitionRequestInfo(
    const TPtr& target,
    const NProto::TReqReadTablePartition& req)
{
    target->SetRequestInfo(
        "Unordered: %v, OmitInaccessibleColumns: %v, DesiredRowsetFormat: %v, ArrowFallbackRowsetFormat: %v",
        req.unordered(),
        req.omit_inaccessible_columns(),
        NProto::ERowsetFormat_Name(req.desired_rowset_format()),
        NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

template <class TPtr>
void SetStartDistributedWriteSessionRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path)
{
    target->SetRequestInfo(
        "Path: %v",
        path);
}

template <class TPtr>
void SetPingDistributedWriteSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId)
{
    target->SetRequestInfo(
        "TableId: %v",
        tableId);
}

template <class TPtr>
void SetFinishDistributedWriteSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId)
{
    target->SetRequestInfo(
        "TableId: %v",
        tableId);
}

template <class TPtr>
void SetWriteTableFragmentRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId)
{
    target->SetRequestInfo(
        "TableId: %v, MainTransactionId: %v",
        tableId,
        mainTransactionId);
}

template <class TPtr>
void SetStartDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    const NYPath::TRichYPath& path)
{
    target->SetRequestInfo(
        "Path: %v",
        path);
}

template <class TPtr>
void SetPingDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId)
{
    target->SetRequestInfo(
        "FileId: %v",
        fileId);
}

template <class TPtr>
void SetFinishDistributedWriteFileSessionRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId)
{
    target->SetRequestInfo(
        "FileId: %v",
        fileId);
}

template <class TPtr>
void SetWriteFileFragmentRequestInfo(
    const TPtr& target,
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId)
{
    target->SetRequestInfo(
        "FileId: %v, MainTransactionId: %v",
        fileId,
        mainTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
