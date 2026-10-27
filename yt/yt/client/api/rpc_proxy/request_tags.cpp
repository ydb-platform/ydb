#include "request_tags.h"

#include <yt/yt/client/api/distributed_file_session.h>
#include <yt/yt/client/api/distributed_table_session.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NRpcProxy {

using NLogging::TLoggingTagList;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLoggingTagList MakeReadTableRequestTags(
    const NYPath::TRichYPath& path,
    const NProto::TReqReadTable& req)
{
    return TLoggingTagList()
        .With("Path", path)
        .With("Unordered", req.unordered())
        .With("OmitInaccessibleColumns", req.omit_inaccessible_columns())
        .With("OmitInaccessibleRows", req.omit_inaccessible_rows())
        .With("DesiredRowsetFormat", NProto::ERowsetFormat_Name(req.desired_rowset_format()))
        .With("ArrowFallbackRowsetFormat", NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

TLoggingTagList MakeReadFileRequestTags(
    const NProto::TReqReadFile& req)
{
    return TLoggingTagList()
        .With("Path", req.path())
        .With("Offset", YT_OPTIONAL_FROM_PROTO(req, offset))
        .With("Length", YT_OPTIONAL_FROM_PROTO(req, length));
}

TLoggingTagList MakeWriteTableRequestTags(
    const NYPath::TRichYPath& path)
{
    return TLoggingTagList()
        .With("Path", path);
}

TLoggingTagList MakeWriteFileRequestTags(
    const NYPath::TRichYPath& path,
    const NProto::TReqWriteFile& req)
{
    return TLoggingTagList()
        .With("Path", path)
        .With("ComputeMD5", req.compute_md5());
}

TLoggingTagList MakePartitionTablesRequestTags(
    const std::vector<NYPath::TRichYPath>& paths,
    const NProto::TReqPartitionTables& req)
{
    return TLoggingTagList()
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

TLoggingTagList MakeReadTablePartitionRequestTags(
    const NProto::TReqReadTablePartition& req)
{
    return TLoggingTagList()
        .With("Unordered", req.unordered())
        .With("OmitInaccessibleColumns", req.omit_inaccessible_columns())
        .With("DesiredRowsetFormat", NProto::ERowsetFormat_Name(req.desired_rowset_format()))
        .With("ArrowFallbackRowsetFormat", NProto::ERowsetFormat_Name(req.arrow_fallback_rowset_format()));
}

TLoggingTagList MakePartitionFileRequestTags(
    const NProto::TReqPartitionFile& req)
{
    static constexpr int MaxLoggedRanges = 3;

    return TLoggingTagList()
        .With("Path", req.path())
        .With("Ranges", MakeShrunkFormattableView(
            req.ranges(),
            [] (TStringBuilderBase* builder, const NProto::TReqPartitionFile::TFileReadRange& range) {
                builder->AppendFormat("[%v, %v)",
                    range.begin(),
                    YT_OPTIONAL_FROM_PROTO(range, end));
            },
            MaxLoggedRanges))
        .With("RangeCount", req.ranges_size())
        .With("FetchCookieNodeDescriptors", req.fetch_cookie_node_descriptors());
}

TLoggingTagList MakeReadFilePartitionRequestTags(
    const NProto::TReqReadFilePartition& req)
{
    return TLoggingTagList()
        .With("CookieSize", req.cookie().size());
}

TLoggingTagList MakeStartDistributedWriteSessionRequestTags(
    const NYPath::TRichYPath& path)
{
    return TLoggingTagList()
        .With("Path", path);
}

TLoggingTagList MakePingDistributedWriteSessionRequestTags(
    NObjectClient::TObjectId tableId)
{
    return TLoggingTagList()
        .With("TableId", tableId);
}

TLoggingTagList MakePingDistributedWriteSessionRequestTags(
    const NTableClient::TSignedDistributedWriteSessionPtr& session)
{
    if (auto payload = TryParseSignedPayload<TDistributedWriteSession>(session.Underlying())) {
        return MakePingDistributedWriteSessionRequestTags(payload->PatchInfo.ObjectId);
    }
    return {};
}

TLoggingTagList MakeFinishDistributedWriteSessionRequestTags(
    NObjectClient::TObjectId tableId)
{
    return TLoggingTagList()
        .With("TableId", tableId);
}

TLoggingTagList MakeFinishDistributedWriteSessionRequestTags(
    const NTableClient::TSignedDistributedWriteSessionPtr& session)
{
    if (auto payload = TryParseSignedPayload<TDistributedWriteSession>(session.Underlying())) {
        return MakeFinishDistributedWriteSessionRequestTags(payload->PatchInfo.ObjectId);
    }
    return {};
}

TLoggingTagList MakeWriteTableFragmentRequestTags(
    NObjectClient::TObjectId tableId,
    NCypressClient::TTransactionId mainTransactionId)
{
    return TLoggingTagList()
        .With("TableId", tableId)
        .With("MainTransactionId", mainTransactionId);
}

TLoggingTagList MakeWriteTableFragmentRequestTags(
    const NTableClient::TSignedWriteFragmentCookiePtr& cookie)
{
    if (auto payload = TryParseSignedPayload<TWriteFragmentCookie>(cookie.Underlying())) {
        return MakeWriteTableFragmentRequestTags(
            payload->PatchInfo.ObjectId,
            payload->MainTransactionId);
    }
    return {};
}

TLoggingTagList MakeStartDistributedWriteFileSessionRequestTags(
    const NYPath::TRichYPath& path)
{
    return TLoggingTagList()
        .With("Path", path);
}

TLoggingTagList MakePingDistributedWriteFileSessionRequestTags(
    NObjectClient::TObjectId fileId)
{
    return TLoggingTagList()
        .With("FileId", fileId);
}

TLoggingTagList MakePingDistributedWriteFileSessionRequestTags(
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session)
{
    if (auto payload = TryParseSignedPayload<TDistributedWriteFileSession>(session.Underlying())) {
        return MakePingDistributedWriteFileSessionRequestTags(payload->HostData.FileId);
    }
    return {};
}

TLoggingTagList MakeFinishDistributedWriteFileSessionRequestTags(
    NObjectClient::TObjectId fileId)
{
    return TLoggingTagList()
        .With("FileId", fileId);
}

TLoggingTagList MakeFinishDistributedWriteFileSessionRequestTags(
    const NFileClient::TSignedDistributedWriteFileSessionPtr& session)
{
    if (auto payload = TryParseSignedPayload<TDistributedWriteFileSession>(session.Underlying())) {
        return MakeFinishDistributedWriteFileSessionRequestTags(payload->HostData.FileId);
    }
    return {};
}

TLoggingTagList MakeWriteFileFragmentRequestTags(
    NObjectClient::TObjectId fileId,
    NCypressClient::TTransactionId mainTransactionId)
{
    return TLoggingTagList()
        .With("FileId", fileId)
        .With("MainTransactionId", mainTransactionId);
}

TLoggingTagList MakeWriteFileFragmentRequestTags(
    const NFileClient::TSignedWriteFileFragmentCookiePtr& cookie)
{
    if (auto payload = TryParseSignedPayload<TWriteFileFragmentCookie>(cookie.Underlying())) {
        return MakeWriteFileFragmentRequestTags(
            payload->CookieData.FileId,
            payload->CookieData.MainTransactionId);
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
