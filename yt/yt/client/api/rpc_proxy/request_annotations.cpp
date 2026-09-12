#include "request_annotations.h"

#include "request_info.h"

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/distributed_table_session.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void AnnotateReadTableRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req)
{
    request->Annotate()
        .With("Path", path)
        .With("Unordered", req.unordered())
        .With("OmitInaccessibleColumns", req.omit_inaccessible_columns())
        .With("OmitInaccessibleRows", req.omit_inaccessible_rows())
        .With("DesiredRowsetFormat", NProto::ERowsetFormat_Name(req.desired_rowset_format()))
        .With("ArrowFallbackRowsetFormat", NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

void AnnotateReadFileRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NProto::TReqReadFile& req)
{
    request->Annotate()
        .With("Path", req.path())
        .With("Offset", YT_OPTIONAL_FROM_PROTO(req, offset))
        .With("Length", YT_OPTIONAL_FROM_PROTO(req, length));
}

void AnnotateWriteTableRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path)
{
    request->Annotate()
        .With("Path", path);
}

void AnnotateWriteFileRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req)
{
    request->Annotate()
        .With("Path", path)
        .With("ComputeMD5", req.compute_md5());
}

void AnnotatePartitionTablesRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req)
{
    request->Annotate()
        .With("Paths", paths)
        .With("PartitionMode", FromProto<NTableClient::ETablePartitionMode>(req.partition_mode()))
        .With("KeyGuarantee", req.enable_key_guarantee())
        .With("DataWeightPerPartition", YT_OPTIONAL_FROM_PROTO(req, data_weight_per_partition))
        .With("CompressedDataSizePerPartition", YT_OPTIONAL_FROM_PROTO(req, compressed_data_size_per_partition))
        .With("MaxPartitionCount", YT_OPTIONAL_FROM_PROTO(req, max_partition_count))
        .With("AdjustDataWeightPerPartition", req.adjust_data_weight_per_partition())
        .With("EnableCookies", req.enable_cookies())
        .With("FetchCookieNodeDescriptors", req.fetch_cookie_node_descriptors())
        .With("OmitInaccessibleRows", req.omit_inaccessible_rows());
}

void AnnotateReadTablePartitionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NProto::TReqReadTablePartition& req)
{
    request->Annotate()
        .With("Unordered", req.unordered())
        .With("OmitInaccessibleColumns", req.omit_inaccessible_columns())
        .With("DesiredRowsetFormat", NProto::ERowsetFormat_Name(req.desired_rowset_format()))
        .With("ArrowFallbackRowsetFormat", NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

void AnnotateStartDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path)
{
    request->Annotate()
        .With("Path", path);
}

void AnnotatePingDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId)
{
    request->Annotate()
        .With("TableId", tableId);
}

void AnnotatePingDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedDistributedWriteSessionPtr& session)
{
    if (auto payload = NDetail::TryParseSignedPayload<TDistributedWriteSession>(session.Underlying())) {
        AnnotatePingDistributedWriteSessionRequestInfo(request, payload->PatchInfo.ObjectId);
    }
}

void AnnotateFinishDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId)
{
    request->Annotate()
        .With("TableId", tableId);
}

void AnnotateFinishDistributedWriteSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedDistributedWriteSessionPtr& session)
{
    if (auto payload = NDetail::TryParseSignedPayload<TDistributedWriteSession>(session.Underlying())) {
        AnnotateFinishDistributedWriteSessionRequestInfo(request, payload->PatchInfo.ObjectId);
    }
}

void AnnotateWriteTableFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId)
{
    request->Annotate()
        .With("TableId", tableId)
        .With("MainTransactionId", mainTransactionId);
}

void AnnotateWriteTableFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NTableClient::TSignedWriteFragmentCookiePtr& cookie)
{
    if (auto payload = NDetail::TryParseSignedPayload<TWriteFragmentCookie>(cookie.Underlying())) {
        AnnotateWriteTableFragmentRequestInfo(
            request,
            payload->PatchInfo.ObjectId,
            payload->MainTransactionId);
    }
}

void AnnotateStartDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NYPath::TRichYPath& path)
{
    request->Annotate()
        .With("Path", path);
}

void AnnotatePingDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId)
{
    request->Annotate()
        .With("FileId", fileId);
}

void AnnotatePingDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session)
{
    if (auto payload = NDetail::TryParseSignedPayload<TDistributedWriteFileSession>(session.Underlying())) {
        AnnotatePingDistributedWriteFileSessionRequestInfo(request, payload->HostData.FileId);
    }
}

void AnnotateFinishDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId)
{
    request->Annotate()
        .With("FileId", fileId);
}

void AnnotateFinishDistributedWriteFileSessionRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session)
{
    if (auto payload = NDetail::TryParseSignedPayload<TDistributedWriteFileSession>(session.Underlying())) {
        AnnotateFinishDistributedWriteFileSessionRequestInfo(request, payload->HostData.FileId);
    }
}

void AnnotateWriteFileFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId)
{
    request->Annotate()
        .With("FileId", fileId)
        .With("MainTransactionId", mainTransactionId);
}

void AnnotateWriteFileFragmentRequestInfo(
    const NRpc::TClientRequestPtr& request,
    const NFileClient::TSignedWriteFileFragmentCookiePtr& cookie)
{
    if (auto payload = NDetail::TryParseSignedPayload<TWriteFileFragmentCookie>(cookie.Underlying())) {
        AnnotateWriteFileFragmentRequestInfo(
            request,
            payload->CookieData.FileId,
            payload->CookieData.MainTransactionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
