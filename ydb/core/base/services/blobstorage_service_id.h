#pragma once

#include "defs.h"
#include <util/generic/string.h>
#include <ydb/core/base/blobstorage_common.h>
namespace NKikimr {

inline TActorId MakeBlobStorageProxyID(ui32 blobStorageGroup) {
    char x[12] = {'b', 's', 'p', 'r', 'o', 'x', 'y' };
    x[7] = (char)blobStorageGroup;
    x[8] = (char)(blobStorageGroup >> 8);
    x[9] = (char)(blobStorageGroup >> 16);
    x[10] = (char)(blobStorageGroup >> 24);
    return TActorId(0, TStringBuf(x, 12));
}

inline TActorId MakeBlobStorageProxyID(TGroupId blobStorageGroup) {
    return MakeBlobStorageProxyID(blobStorageGroup.GetRawId());
}

inline TActorId MakeLoadServiceID(ui32 nodeId) {
    char x[12] = {'l', 'o', 'a', 'd', 't', 'e', 's', 't'};
    x[8] = (char)(nodeId >> 24);
    x[9] = (char)(nodeId >> 16);
    x[10] = (char)(nodeId >> 8);
    x[11] = (char)nodeId;
    return TActorId(nodeId, TStringBuf(x, 12));
}

inline TActorId MakeBlobStorageFailureInjectionID(ui32 nodeId) {
    char x[12] = {'b', 's', 'F', 'a', 'i', 'l', 'I', 'n'};
    x[8] = (char)(nodeId >> 24);
    x[9] = (char)(nodeId >> 16);
    x[10] = (char)(nodeId >> 8);
    x[11] = (char)nodeId;
    return TActorId(nodeId, TStringBuf(x, 12));
}

inline TActorId MakeBlobStorageVDiskID(ui32 node, ui32 pDiskID, ui32 vDiskSlotID) {
    char x[12] = {'b','s','v','d'};
    x[4] = (char)pDiskID;
    x[5] = (char)(pDiskID >> 8);
    x[6] = (char)(pDiskID >> 16);
    x[7] = (char)(pDiskID >> 24);
    x[8] = (char)vDiskSlotID;
    x[9] = (char)(vDiskSlotID >> 8);
    x[10] = (char)(vDiskSlotID >> 16);
    x[11] = (char)(vDiskSlotID >> 24);
    return TActorId(node, TStringBuf(x, 12));
}

inline std::tuple<ui32, ui32, ui32> DecomposeVDiskServiceId(const TActorId& actorId) {
    Y_ABORT_UNLESS(actorId.IsService());
    const TStringBuf serviceId = actorId.ServiceId();
    const ui8 *ptr = reinterpret_cast<const ui8*>(serviceId.data());
    Y_ABORT_UNLESS(serviceId.size() == 12);
    Y_ABORT_UNLESS(memcmp(ptr, "bsvd", 4) == 0);
    const ui32 nodeId = actorId.NodeId();
    const ui32 pdiskId = (ui32)ptr[4] | (ui32)ptr[5] << 8 | (ui32)ptr[6] << 16 | (ui32)ptr[7] << 24;
    const ui32 vslotId = (ui32)ptr[8] | (ui32)ptr[9] << 8 | (ui32)ptr[10] << 16 | (ui32)ptr[11] << 24;
    Y_DEBUG_ABORT_UNLESS(actorId == MakeBlobStorageVDiskID(nodeId, pdiskId, vslotId));
    return {nodeId, pdiskId, vslotId};
}

inline TActorId MakeBlobStoragePDiskID(ui32 node, ui32 pDiskID) {
    char x[12] = {'b','s','p','d','i','s','k', 0};
    x[8] = (char)pDiskID;
    x[9] = (char)(pDiskID >> 8);
    x[10] = (char)(pDiskID >> 16);
    x[11] = (char)(pDiskID >> 24);
    return TActorId(node, TStringBuf(x, 12));
}

inline TActorId MakeBlobStorageReplBrokerID() {
    char x[12] = {'b', 's', 'r', 'e', 'p', 'l', 'b', 'r', 'o', 'k', 'e', 'r'};
    return TActorId(0, TStringBuf(x, 12));
}

inline TActorId MakeBlobStorageNodeWardenID(ui32 node) {
    char x[12] = {'b','s','n','o','d','e','c','n','t','r','l','r'};
    return TActorId(node, TStringBuf(x, 12));
}

} // namespace NKikimr
