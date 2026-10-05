#pragma once

#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

namespace NKikimr::NCms {

using TPDiskStateInfo = NKikimrWhiteboard::TPDiskStateInfo;
using EPDiskState = NKikimrBlobStorage::TPDiskState::E;

// Whether a PDisk reporting the given Whiteboard state should be considered
// reachable/available. This intentionally treats the initial-startup states
// (Initial, InitialFormatRead, InitialSysLogRead, InitialCommonLogRead) as
// available: they only mean the PDisk is still starting up and hasn't failed
// yet, unlike their *Error counterparts. Only states that indicate the node
// itself is unreachable (Timeout, NodeDisconnected) or that the PDisk has
// hit a genuine hard failure (the various Initial*Error/*Error states,
// CommonLoggerInitError, Stopped) are treated as unavailable. Missing and
// the reserved/unknown states are conservatively treated as unavailable
// since there is no positive evidence the disk is working.
inline bool IsPDiskStateUp(EPDiskState state) {
    switch (state) {
        case NKikimrBlobStorage::TPDiskState::Normal:
        case NKikimrBlobStorage::TPDiskState::Initial:
        case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
        case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
        case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
            return true;
        default:
            return false;
    }
}

} // namespace NKikimr::NCms
