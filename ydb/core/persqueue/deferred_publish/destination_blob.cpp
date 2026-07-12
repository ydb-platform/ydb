#include "destination_blob.h"

namespace NKikimr::NPQ::NDeferredPublish {

TString SerializeDestinationBlob(const NKikimrPQ::TDeferredPublishDestinationBlob& blob) {
    TString bytes;
    Y_PROTOBUF_SUPPRESS_NODISCARD blob.SerializeToString(&bytes);
    return bytes;
}

bool ParseDestinationBlob(TStringBuf bytes, NKikimrPQ::TDeferredPublishDestinationBlob* blob) {
    Y_ABORT_UNLESS(blob != nullptr);
    return blob->ParseFromArray(bytes.data(), bytes.size());
}

NKikimrPQ::TDeferredPublishDestinationBlob MakeDestinationBlob(
    const TString& path,
    ui32 partitionId,
    ui64 tabletId)
{
    NKikimrPQ::TDeferredPublishDestinationBlob blob;
    blob.SetPath(path);
    blob.SetPartitionId(partitionId);
    blob.SetTabletId(tabletId);
    return blob;
}

} // namespace NKikimr::NPQ::NDeferredPublish
