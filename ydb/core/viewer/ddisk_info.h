#pragma once

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/protos/node_whiteboard.pb.h>

#include <util/string/printf.h>

namespace NKikimr::NViewer {

inline void FillDDiskIdentity(NKikimrWhiteboard::TDDiskStateInfo& info, ui32 nodeId) {
    info.SetNodeId(nodeId);
    if (!info.HasPersistentBufferId()) {
        info.SetPersistentBufferId(MakeBlobStoragePersistentBufferId(
            nodeId, info.GetPDiskId(), info.GetDDiskSlotId()).ToString());
    }
    info.SetDDiskPath(Sprintf("actors/ddisks/ddisk_p%09" PRIu32 "_s%09" PRIu32,
        info.GetPDiskId(), info.GetDDiskSlotId()));
}

} // namespace NKikimr::NViewer
