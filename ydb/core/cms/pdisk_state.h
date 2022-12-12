#pragma once

#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr::NCms {

using TPDiskStateInfo = NKikimrWhiteboard::TPDiskStateInfo;
using EPDiskState = NKikimrBlobStorage::TPDiskState::E;

} // namespace NKikimr::NCms
