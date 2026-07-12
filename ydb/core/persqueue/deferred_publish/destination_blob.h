#pragma once

#include <ydb/core/protos/pqdata_deferred_publish_destination.pb.h>

namespace NKikimr::NPQ::NDeferredPublish {

TString SerializeDestinationBlob(const NKikimrPQ::TDeferredPublishDestinationBlob& blob);
bool ParseDestinationBlob(TStringBuf bytes, NKikimrPQ::TDeferredPublishDestinationBlob* blob);

NKikimrPQ::TDeferredPublishDestinationBlob MakeDestinationBlob(
    const TString& path,
    ui32 partitionId,
    ui64 tabletId);

} // namespace NKikimr::NPQ::NDeferredPublish
